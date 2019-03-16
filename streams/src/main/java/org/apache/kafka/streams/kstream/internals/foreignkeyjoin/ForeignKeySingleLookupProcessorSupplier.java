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

public class ForeignKeySingleLookupProcessorSupplier<K, KO, V, VO>
        implements ProcessorSupplier<CombinedKey<KO, K>, SubscriptionWrapper> {

    private final String topicName;
    private final KTableValueGetterSupplier<KO, VO> foreignValueGetterSupplier;

    public ForeignKeySingleLookupProcessorSupplier(final String topicName,
                                                   final KTableValueGetterSupplier<KO, VO> foreignValueGetter) {
        this.topicName = topicName;
        this.foreignValueGetterSupplier = foreignValueGetter;
    }

    @Override
    public Processor<CombinedKey<KO, K>, SubscriptionWrapper> get() {

        return new AbstractProcessor<CombinedKey<KO, K>, SubscriptionWrapper>() {

            private KeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper> store;
            private KTableValueGetter<KO, VO> foreignValues;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                foreignValues = foreignValueGetterSupplier.get();
                foreignValues.init(context);
                store = (KeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper>) context.getStateStore(topicName);
            }

            @Override
            public void process(final CombinedKey<KO, K> key, final SubscriptionWrapper value) {
                VO foreignValue = foreignValues.get(key.getForeignKey());

                //TODO - Bellemare - If the subscriptionWrapper indicates a null, must delete.
                if (value.getHash() == null) {
                    store.delete(key);
                } else {
                    store.put(key, value);
                }

                //What to do if the foreign key actually is null? Can't partition correctly...
                if ((value.getHash() != null && foreignValue != null) ||
                    (value.getHash() == null && value.isPropagate())) {
                    final SubscriptionResponseWrapper<VO> newValue = new SubscriptionResponseWrapper<>(value.getHash(), foreignValue);
                    context().forward(key.getPrimaryKey(), newValue);
                }
            }
        };
    }


    public KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, SubscriptionWrapper> valueGetterSupplier() {
        return new KTableSourceValueGetterSupplier<>(topicName);
    }
}