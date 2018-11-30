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
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.UserRecordHeaderIsolatorUtil.removeUserHeaderPrefix;

public class SourceResolverProcessorSupplier<K, KO, V, VR> implements ProcessorSupplier<CombinedKey<KO, K>, VR> {
    private final String stateStoreName;
    private final ValueMapper<V, KO> keyExtractor;

    public SourceResolverProcessorSupplier(final String stateStoreName,
                                           final ValueMapper<V, KO> keyExtractor) {
        this.stateStoreName = stateStoreName;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public Processor<CombinedKey<KO, K>, VR> get() {
        return new AbstractProcessor<CombinedKey<KO, K>, VR>() {
            private KeyValueStore<K, V> originalSource;

            @Override
            public void init(final ProcessorContext context) {
                super.init(context);
                this.originalSource = (KeyValueStore<K, V>) context.getStateStore(stateStoreName);
            }

            @Override
            public void process(CombinedKey<KO, K> key, VR value) {
                //Validate that keyExtractor(V) == CombinedKey<KO,_>

                final V ogVal = originalSource.get(key.getPrimaryKey());

                final KO ogExtractedKey = keyExtractor.apply(ogVal);
                final KO foreignKey = key.getForeignKey();

                //Forward if deleted (both null)
                //Forward if it matches
                if (ogVal == null && value == null) {
                    context().forward(key.getPrimaryKey(), value);
                } else if (foreignKey.equals(ogExtractedKey)) {
                    context().forward(key.getPrimaryKey(), value);
                }
            }
        };
    }
}
