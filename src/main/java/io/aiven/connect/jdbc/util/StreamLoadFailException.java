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

public class StreamLoadFailException extends Exception {

    public StreamLoadFailException() {
        super();
    }

    public StreamLoadFailException(final String message) {
        super(message);
    }

    public StreamLoadFailException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public StreamLoadFailException(final Throwable cause) {
        super(cause);
    }

    protected StreamLoadFailException(final String message,
                                      final Throwable cause,
                                      final boolean enableSuppression,
                                      final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
