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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.util.StarRocksConstants.CsvFormat;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamLoadStream extends InputStream {

    private static final Logger log = LoggerFactory.getLogger(StreamLoadStream.class);

    private static final int DEFAULT_BUFFER_SIZE = 2048;

    private final Iterator<SinkRecord> recordIter;

    private ByteBuffer buffer;
    private byte[] cache;
    private int pos;
    private boolean endStream = false;

    public StreamLoadStream(final Iterator<SinkRecord> recordIter) {
        this.recordIter = recordIter;

        buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        buffer.position(buffer.capacity());
    }


    @Override
    public int read() throws IOException {
        final byte[] bytes = new byte[1];
        final int ws = read(bytes);
        if (ws == -1) {
            return -1;
        }
        return bytes[0];
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        if (!buffer.hasRemaining()) {
            if (cache == null && endStream) {
                return -1;
            }
            fillBuffer();
        }

        final int size = len - off;
        final int ws = Math.min(size, buffer.remaining());
        buffer.get(b, off, ws);

        return ws;
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        cache = null;
        pos = 0;
        endStream = true;
    }

    private void fillBuffer() {
        buffer.clear();
        if (cache != null) {
            writeBuffer(cache, pos);
        }

        if (endStream || !buffer.hasRemaining()) {
            buffer.flip();
            return;
        }

        byte[] bytes;
        while ((bytes = nextCsvRow()) != null) {
            writeBuffer(bytes, 0);
            bytes = null;
            if (!buffer.hasRemaining()) {
                break;
            }
        }
        if (buffer.position() == 0) {
            buffer.position(buffer.limit());
        } else {
            buffer.flip();
        }
    }

    private void writeBuffer(final byte[] bytes, final int pos) {
        final int size = bytes.length - pos;

        final int remain = buffer.remaining();

        final int ws = Math.min(size, remain);
        buffer.put(bytes, pos, ws);
        if (size > remain) {
            this.cache = bytes;
            this.pos = pos + ws;
        } else {
            this.cache = null;
            this.pos = 0;
        }
    }


    private byte[] nextCsvRow() {
        
        if (recordIter.hasNext()) {
            final SinkRecord record = recordIter.next();
            
            final JSONObject j = new JSONObject();
            // String j  = "";
            final Struct s = (Struct) record.value();
            // boolean first = true;
            for (final Field f: s.schema().fields()) {
                // if (!first) {
                //     j += StarRocksDelimiterParser.parse(CsvFormat.COLUMN_DELIMITER);
                // } else {
                //     first = false;
                // }
                // j += s.get(f).toString();
                
                j.put(f.name(), s.get(f));
            }
            final String w = String.format(CsvFormat.LINE_PATTERN, j.toString());
            log.info(w);
            return w.getBytes(StandardCharsets.UTF_8);
            // return String.format(CsvFormat.LINE_PATTERN, j).getBytes(StandardCharsets.UTF_8);
            
        }

        endStream = true;
        return CsvFormat.LINE_DELIMITER_BYTE;
    }
}
