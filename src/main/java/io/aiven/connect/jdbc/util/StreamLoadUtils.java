/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

package io.aiven.connect.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
// import org.apache.http.impl.client.CloseableHttpClient;
// import org.apache.http.impl.client.DefaultRedirectStrategy;
// import org.apache.http.impl.client.HttpClientBuilder;
// import org.apache.http.impl.client.HttpClients;
// import io.airbyte.integrations.destination.starrocks.StarRocksConstants;

// import org.apache.kafka.common.config.types.Password;

public class StreamLoadUtils {
    // private final HttpClientBuilder httpClientBuilder =
    //         HttpClients
    //                 .custom()
    //                 .setRedirectStrategy(new DefaultRedirectStrategy() {

    //                     @Override
    //                     protected boolean isRedirectable(String method) {
    //                         return true;
    //                     }

    //                 });

    // public CloseableHttpClient getClient() {
    //     return httpClientBuilder.build();
    // }


    // public static String getTableUniqueKey(String database, String table) {
    //     return database + "-" + table;
    // }

    // public static String getStreamLoadUrl(String host, String database, String table) {
    //     if (host == null) {
    //         throw new IllegalArgumentException("None of the hosts in `load_url` could be connected.");
    //     }
    //     return host +
    //             "/api/" +
    //             database +
    //             "/" +
    //             table +
    //             "/_stream_load";
    // }

    public static String label(final String table) {
        return "kafka_" + table + "_" + UUID.randomUUID() + System.currentTimeMillis();
    }


    public static String getBasicAuthHeader(final String username, final String password) {
        final String auth = username + ":" + password;
        final byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth, StandardCharsets.UTF_8);
    }

    public static Header[] getHeaders(final String user, final String pwd) {
        final Map<String, String> headers = new HashMap<>();
        headers.put("timeout", "600");
        headers.put(HttpHeaders.AUTHORIZATION, StreamLoadUtils.getBasicAuthHeader(user, pwd));
        headers.put(HttpHeaders.EXPECT, "100-continue");
        headers.put("column_separator", StarRocksConstants.CsvFormat.COLUMN_DELIMITER);
        headers.put("row_delimiter", StarRocksConstants.CsvFormat.LINE_DELIMITER);
        headers.put("format", "json");
        return headers.entrySet().stream()
                .map(entry -> new BasicHeader(entry.getKey(), entry.getValue()))
                .toArray(Header[]::new);
    }
}
