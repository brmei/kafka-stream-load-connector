/*
 * Copyright 2020 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.sink;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.util.StarRocksConstants;
import io.aiven.connect.jdbc.util.StreamLoadEntity;
import io.aiven.connect.jdbc.util.StreamLoadFailException;
import io.aiven.connect.jdbc.util.StreamLoadResponse;
import io.aiven.connect.jdbc.util.StreamLoadUtils;

import com.alibaba.fastjson.JSON;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {

    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

    private static final int ERROR_LOG_MAX_LENGTH = 3000;

    private static final Pattern NORMALIZE_TABLE_NAME_FOR_TOPIC = Pattern.compile("[^a-zA-Z0-9_]");

    private final JdbcSinkConfig config;

    private final String host;
    private final int port;
    final String database;
    final boolean ssl;
    private final String username;
    private final String password;

    JdbcDbWriter(final JdbcSinkConfig config) {
        this.config = config;
        this.host = config.getConnectionHost();
        this.port = config.getConnectionPort();
        this.database = config.getConnectionDatabase();
        this.ssl = config.getConnectionSSL();
        this.username = config.getConnectionUser();
        this.password = config.getConnectionPassword();
    }

    void write(final Collection<SinkRecord> records) throws Exception {
        final String dbTable = generateTableNameFor(records.iterator().next().topic());

        String scheme = "http";
        // check if port for appropriate url scheme
        if (ssl) {
            scheme = "https";
        }

        final String loadUrlPath = String.format(StarRocksConstants.PATTERN_PATH_STREAM_LOAD, database, dbTable);
        final String sendUrl = String.format("%s://%s:%d%s", scheme, host, port, loadUrlPath);
        final String label = StreamLoadUtils.label(dbTable);
        final HttpPut httpPut = new HttpPut(sendUrl);

        httpPut.setEntity(new StreamLoadEntity(records));
        log.info(EntityUtils.toString(httpPut.getEntity()));

        final Header[] defaultHeaders = StreamLoadUtils.getHeaders(username, password);
        httpPut.setHeaders(defaultHeaders);
        
        for (final Header e: httpPut.getAllHeaders()) {
            log.info("Header: {}", e.toString());
        }

        log.info("Stream loading, label : {}, database : {}, table : {}, request : {}",
                label, database, dbTable, httpPut);

        String responseBody = null;

        try (CloseableHttpClient client = HttpClients.createDefault();) {
            final long startNanoTime = System.nanoTime();
            try (CloseableHttpResponse response = client.execute(httpPut)) {
                final HttpEntity responseEntity = response.getEntity();
                responseBody = EntityUtils.toString(responseEntity);
            }
            final StreamLoadResponse streamLoadResponse = new StreamLoadResponse();
            final StreamLoadResponse.StreamLoadResponseBody streamLoadBody
                    = JSON.parseObject(responseBody, StreamLoadResponse.StreamLoadResponseBody.class);
            streamLoadResponse.setBody(streamLoadBody);

            final String status = streamLoadBody.getStatus();

            if (StarRocksConstants.RESULT_STATUS_SUCCESS.equals(status)
                    || StarRocksConstants.RESULT_STATUS_OK.equals(status)
                    || StarRocksConstants.RESULT_STATUS_TRANSACTION_PUBLISH_TIMEOUT.equals(status)) {
                streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                log.info("Stream load completed, label : {}, database : {}, table : {}, body : {}",
                        label, database, dbTable, responseBody);

            } else if (StarRocksConstants.RESULT_STATUS_LABEL_EXISTED.equals(status)) {
                final boolean succeed = checkLabelState(host, database, label);
                if (succeed) {
                    streamLoadResponse.setCostNanoTime(System.nanoTime() - startNanoTime);
                    log.info("Stream load completed, label : {}, database : {}, table : {}, body : {}",
                            label, database, dbTable, responseBody);
                } else {
                    final String errorMsg = String.format("Stream load failed because label existed, "
                        + "db: %s, table: %s, label: %s", database, dbTable, label);
                    throw new StreamLoadFailException(errorMsg);
                }
            } else {
                final String errorLog = getErrorLog(streamLoadBody.getErrorURL());
                final String errorMsg = String.format(
                        "Stream load failed because of error, db: %s, table: %s, label: %s, "
                        + "\nresponseBody: %s\nerrorLog: %s", database, dbTable, label,
                        responseBody, errorLog);
                throw new StreamLoadFailException(errorMsg);
            }
        } catch (final StreamLoadFailException e) {
            throw e;
        }  catch (final Exception e) {
            log.error("error response from stream load: \n" + responseBody);
            final String errorMsg = String.format(
                    "Stream load failed because of unknown exception, db: %s, table: %s, "
                    + "label: %s", database, dbTable, label);
            throw new StreamLoadFailException(errorMsg, e);
        }
    }

    void closeQuietly() {
    //     cachedConnectionProvider.close();
    }

    // TableId destinationTable(final String topic) {
    //     final String tableName = generateTableNameFor(records.iterator().next().topic());
    //     // return dbDialect.parseTableIdentifier(tableName);
    //     return tableName;
    // }

    public String generateTableNameFor(final String topic) {
        String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (config.tableNameNormalize) {
            tableName = NORMALIZE_TABLE_NAME_FOR_TOPIC.matcher(tableName).replaceAll("_");
        }
        if (!config.topicsToTablesMapping.isEmpty()) {
            tableName = config.topicsToTablesMapping.getOrDefault(topic, "");
        }
        if (tableName.isEmpty()) {
            final String errorMessage =
                    String.format(
                            "Destination table for the topic: '%s' "
                                    + "couldn't be found in the topics to tables mapping: '%s' "
                                    + "and couldn't be generated for the format string '%s'",
                            topic,
                            config.topicsToTablesMapping
                                    .entrySet()
                                    .stream()
                                    .map(e -> String.join("->", e.getKey(), e.getValue()))
                                    .collect(Collectors.joining(",")),
                            config.tableNameFormat);
            throw new ConnectException(errorMessage);
        }
        return tableName;
    }

    protected boolean checkLabelState(final String host, final String database, final String label) throws Exception {
        int idx = 0;
        for (;;) {
            TimeUnit.SECONDS.sleep(Math.min(++idx, 5));
            try (CloseableHttpClient client = HttpClients.createDefault()) {
                final String url = host + "/api/" + database + "/get_load_state?label=" + label;
                final HttpGet httpGet = new HttpGet(url);
                httpGet.addHeader("Authorization",
                        StreamLoadUtils.getBasicAuthHeader(username, password));
                httpGet.setHeader("Connection", "close");
                try (CloseableHttpResponse response = client.execute(httpGet)) {
                    final String entityContent = EntityUtils.toString(response.getEntity());

                    if (response.getStatusLine().getStatusCode() != 200) {
                        throw new StreamLoadFailException("Failed to flush data to StarRocks, Error "
                                + "could not get the final state of label : `" + label + "`, body : " + entityContent);
                    }

                    log.info("Label `{}` check, body : {}", label, entityContent);
                    final StreamLoadResponse.StreamLoadResponseBody responseBody =
                            JSON.parseObject(entityContent, StreamLoadResponse.StreamLoadResponseBody.class);
                    final String state = responseBody.getState();
                    if (state == null) {
                        log.error("Get label state failed, body : {}", JSON.toJSONString(responseBody));
                        throw new StreamLoadFailException(String.format("Failed to flush data to StarRocks, Error "
                                + "could not get the final state of label[%s]. response[%s]\n", label, entityContent));
                    }
                    switch (state) {
                        case StarRocksConstants.LABEL_STATE_VISIBLE:
                        case StarRocksConstants.LABEL_STATE_PREPARED:
                        case StarRocksConstants.LABEL_STATE_COMMITTED:
                            return true;
                        case StarRocksConstants.LABEL_STATE_PREPARE:
                            continue;
                        case StarRocksConstants.LABEL_STATE_ABORTED:
                            return false;
                        case StarRocksConstants.LABEL_STATE_UNKNOWN:
                        default:
                            throw new StreamLoadFailException(String.format("Failed to flush data to StarRocks, Error "
                                    + "label[%s] state[%s]\n", label, state));
                    }
                }
            }
        }
    }

    protected String getErrorLog(final String errorUrl) {
        if (errorUrl == null || !errorUrl.startsWith(HttpHost.DEFAULT_SCHEME_NAME)) {
            return null;
        }

        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            final HttpGet httpGet = new HttpGet(errorUrl);
            try (CloseableHttpResponse resp = httpclient.execute(httpGet)) {
                final int code = resp.getStatusLine().getStatusCode();
                if (HttpStatus.SC_OK != code) {
                    log.warn("Request error log failed with error code: {}, errorUrl: {}", code, errorUrl);
                    return null;
                }

                final HttpEntity respEntity = resp.getEntity();
                if (respEntity == null) {
                    log.warn("Request error log failed with null entity, errorUrl: {}", errorUrl);
                    return null;
                }
                String errorLog = EntityUtils.toString(respEntity);
                if (errorLog != null && errorLog.length() > ERROR_LOG_MAX_LENGTH) {
                    errorLog = errorLog.substring(0, ERROR_LOG_MAX_LENGTH);
                }
                return errorLog;
            }
        } catch (final Exception e) {
            log.warn("Failed to get error log: {}.", errorUrl, e);
            return String.format("Failed to get error log: %s, exception message: %s", errorUrl, e.getMessage());
        }
    }
}
