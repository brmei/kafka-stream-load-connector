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

import java.io.StringWriter;

public class StarRocksDelimiterParser {

    private static final String HEX_STRING = "0123456789ABCDEF";

    public static String parse(final String sp) throws RuntimeException {
        if (sp == null || sp.length() == 0) {
            throw new IllegalArgumentException("Delimiter is null or empty.");
        }
        if (!sp.toUpperCase().startsWith("\\X")) {
            return sp;
        }
        final String hexStr = sp.substring(2);
        // check hex str
        if (hexStr.isEmpty()) {
            throw new RuntimeException("Failed to parse delimiter: `Hex str is empty`");
        }
        if (hexStr.length() % 2 != 0) {
            throw new RuntimeException("Failed to parse delimiter: `Hex str length error`");
        }
        for (final char hexChar : hexStr.toUpperCase().toCharArray()) {
            if (HEX_STRING.indexOf(hexChar) == -1) {
                throw new RuntimeException("Failed to parse delimiter: `Hex str format error`");
            }
        }
        // transform to separator
        final StringWriter writer = new StringWriter();
        for (final byte b : hexStrToBytes(hexStr)) {
            writer.append((char) b);
        }
        return writer.toString();
    }

    private static byte[] hexStrToBytes(final String hexStr) {
        final String upperHexStr = hexStr.toUpperCase();
        final int length = upperHexStr.length() / 2;
        final char[] hexChars = upperHexStr.toCharArray();
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            final int pos = i * 2;
            bytes[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bytes;
    }

    private static byte charToByte(final char c) {
        return (byte) HEX_STRING.indexOf(c);
    }

}
