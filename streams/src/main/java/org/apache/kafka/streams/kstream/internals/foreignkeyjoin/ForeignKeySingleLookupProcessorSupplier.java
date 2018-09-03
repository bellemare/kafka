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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableSourceValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.KTableValueGetter;
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ForeignKeySingleLookupProcessorSupplier<K, KO, V, VO, VR>
        implements ProcessorSupplier<CombinedKey<KO, K>, V> {

    private final String topicName;
    private final KTableValueGetterSupplier<KO, VO> foreignValueGetterSupplier;
    private final ValueJoiner<V, VO, VR> joiner;

    public ForeignKeySingleLookupProcessorSupplier(final String topicName,
                                                   final KTableValueGetterSupplier<KO, VO> foreignValueGetter,
                                                   final ValueJoiner<V, VO, VR> joiner) {
        this.topicName = topicName;
        this.joiner = joiner;
        this.foreignValueGetterSupplier = foreignValueGetter;
    }

    @Override
    public Processor<CombinedKey<KO, K>, V> get() {

        return new AbstractProcessor<CombinedKey<KO, K>, V>() {

            private KeyValueStore<CombinedKey<KO, K>, V> store;
            private KTableValueGetter<KO, VO> foreignValues;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                foreignValues = foreignValueGetterSupplier.get();
                foreignValues.init(context);
                store = (KeyValueStore<CombinedKey<KO, K>, V>) context.getStateStore(topicName);
            }

            @Override
            public void process(final CombinedKey<KO, K> key, final V value) {
                final Headers readHead = context().headers();
                final byte[] propagateBytes = readHead.lastHeader(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString()).value();
                final boolean propagate = propagateBytes[0] == 1;

                //We don't want to propagate a null due to a foreign-key change past this point.
                //Propagation of the updated value will occur in a different partition. State store needs deletion.
                if (!propagate) {
                    store.delete(key);
                    return;
                }

                final V oldVal = store.get(key);
                store.put(key, value);

                VR newValue = null;
                VR oldValue = null;
                VO value2 = null;

                if (value != null || oldVal != null) {
                    final KO foreignKey = key.getForeignKey();
                    value2 = foreignValues.get(foreignKey);
                }

                if (value != null && value2 != null)
                    newValue = joiner.apply(value, value2);

                if (oldVal != null && value2 != null)
                    oldValue = joiner.apply(oldVal, value2);

                if (oldValue != null || newValue != null) {
                    final K realKey = key.getPrimaryKey();
                    context().forward(realKey, newValue);
                }
            }
        };
    }


    public KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, V> valueGetterSupplier() {
        return new KTableSourceValueGetterSupplier<>(topicName);
    }
}