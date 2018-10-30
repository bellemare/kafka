/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.ProcessorContext;

class UserRecordHeaderIsolatorUtil {
    private final static String USER_HEADER = "x";

    static void addUserHeaderPrefix(final ProcessorContext context) {
        final Header[] originalHeaders = context.headers().toArray();
        for (int i = 0; i < originalHeaders.length; ++i) {
            final Header header = originalHeaders[i];
            context.headers().remove(header.key());
            context.headers().add(USER_HEADER + header.key(), header.value());
        }
    }

    static void removeUserHeaderPrefix(final ProcessorContext context) {
        final Header[] prefixedHeaders = context.headers().toArray();
        for (int i = 0; i < prefixedHeaders.length; ++i) {
            final Header header = prefixedHeaders[i];
            context.headers().remove(header.key());
            context.headers().add(header.key().substring(USER_HEADER.length(), header.key().length()), header.value());
        }
    }
}
