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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.UserRecordHeaderIsolatorUtil.removeUserHeaderPrefix;

public class HighwaterResolverProcessorSupplier<K, VR> implements ProcessorSupplier<K, VR> {
    private final String stateStoreName;

    public HighwaterResolverProcessorSupplier(final String stateStoreName) {
        this.stateStoreName = stateStoreName;
    }

    @Override
    public Processor<K, VR> get() {
        return new AbstractProcessor<K, VR>() {
            private final Deserializer<Long> longSerde = Serdes.Long().deserializer();
            private KeyValueStore<K, Long> offsetHighWaterStore;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                this.offsetHighWaterStore = (KeyValueStore<K, Long>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(final K key, final VR value) {
                //highwater = x, value(offset = x+1, null)      => update & forward
                //highwater = x, value(offset = x+1, non-null)  => update & forward
                //highwater = x, value(offset = x, null)        => update & forward. May occur if there is a system failure and we are restoring.
                //highwater = x, value(offset = x, non-null)    => update & forward. May occur if there is a system failure and we are restoring.
                //highwater = x, value(offset = x-1, null)      => Do not send, it is an out-of-order, old update.
                //highwater = x, value(offset = x-1, non-null)  => Do not send, it is an out-of-order, old update.

                final byte[] offsetHeader = context().headers().lastHeader(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString()).value();
                final Long offset = longSerde.deserialize(null, offsetHeader);
                final Long highwater = offsetHighWaterStore.get(key);

                context().headers().remove(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString());
                context().headers().remove(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString());
                removeUserHeaderPrefix(context());
                if (null == highwater || offset >= highwater) {
                    // using greater-than to capture new highwater events.
                    // using equal as we want to resend in the case of a node failure.
                    offsetHighWaterStore.put(key, offset);
                    context().forward(key, value);
                } else if (offset == -1L) {
                    //TODO - Potentially use a different header type to forward from KTableKTablePrefixScanJoin.
                    context().forward(key, value);
                }
            }
        };
    }
}
