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
package org.apache.kafka.streams.kstream.internals;

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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class KTablePrefixValueGetterSupplier<K, V> implements KTableValueGetterSupplier<K, V> {
    private final String storeName;

    public KTablePrefixValueGetterSupplier(final String storeName) {
        this.storeName = storeName;
    }

    public KTablePrefixValueGetter<K, V> get() {
        return new KTablePrefixValueGetterImpl();
    }

    @Override
    public String[] storeNames() {
        return new String[]{storeName};
    }

    private class KTablePrefixValueGetterImpl implements KTablePrefixValueGetter<K, V> {
        private TimestampedKeyValueStore<K, V> store = null;

        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
        }

        public ValueAndTimestamp<V> get(final K key) {
            return store.get(key);
        }

        @Override
        public void close() {
        }

        @Override
        public KeyValueIterator<K, ValueAndTimestamp<V>> prefixScan(final K prefix) {
            return store.prefixScan(prefix);
        }
    }
}
