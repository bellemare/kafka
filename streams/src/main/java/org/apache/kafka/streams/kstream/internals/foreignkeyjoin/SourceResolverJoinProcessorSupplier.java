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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Murmur3;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class SourceResolverJoinProcessorSupplier<K, V, VO, VR> implements ProcessorSupplier<K, SubscriptionResponseWrapper<VO>> {
    private final String stateStoreName;
    private final Serializer<V> valueSerializer;
    private final ValueJoiner<V, VO, VR> joiner;

    public SourceResolverJoinProcessorSupplier(final String stateStoreName,
                                               final Serializer<V> valueSerializer,
                                               final ValueJoiner<V, VO, VR> joiner) {
        this.stateStoreName = stateStoreName;
        this.valueSerializer = valueSerializer;
        this.joiner = joiner;
    }

    @Override
    public Processor<K, SubscriptionResponseWrapper<VO>> get() {
        return new AbstractProcessor<K, SubscriptionResponseWrapper<VO>>() {
            private KeyValueStore<K, V> originalSource;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                this.originalSource = (KeyValueStore<K, V>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(K key, SubscriptionResponseWrapper<VO> value) {
                final V currentValue = originalSource.get(key);

                long[] currentHash = (currentValue == null ?
                        Murmur3.hash128(new byte[]{}):
                        //An extremely bad "workaround" for externalized testing with confluent avro serde.
                        //If using just null, the schema is registered to <null>-topic -> "-topic", and of course
                        //we do not want that at all. Will be removed in final version, but this just illustrates the
                        //main issue with the confluent avro serde.
                        //Murmur3.hash128(valueSerializer.serialize(new Random().nextLong() + "", currentValue)));
                        Murmur3.hash128(valueSerializer.serialize(null, currentValue)));

                final long[] messageHash = value.getOriginalValueHash();

                //If this value doesn't match the current value from the original table, it is stale and should be discarded.
                if (java.util.Arrays.equals(messageHash, currentHash)) {
                    final VO otherValue = value.getForeignValue();
                    //Inner Join
                    VR result = null;
                    if (otherValue != null && currentValue != null) {
                        result = joiner.apply(currentValue, otherValue);
                    }
                    context().forward(key, result);
                }
            }
        };
    }
}
