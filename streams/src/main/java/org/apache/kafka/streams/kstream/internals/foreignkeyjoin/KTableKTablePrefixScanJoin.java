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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetter;
import org.apache.kafka.streams.kstream.internals.KTablePrefixValueGetterSupplier;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;

public class KTableKTablePrefixScanJoin<K, KO, V, VO, VR> implements ProcessorSupplier<KO, Change<VO>> {
    private final ValueJoiner<V, VO, VR> joiner;
    private final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, V> primary;
    private final StateStore ref;

    public KTableKTablePrefixScanJoin(final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, V> primary,
                                      final ValueJoiner<V, VO, VR> joiner,
                                      final StateStore ref) {
        this.primary = primary;
        this.joiner = joiner;
        this.ref = ref;
    }

    @Override
    public Processor<KO, Change<VO>> get() {
        return new KTableKTableJoinProcessor(primary);
    }


    private class KTableKTableJoinProcessor extends AbstractProcessor<KO, Change<VO>> {

        private final KTablePrefixValueGetter<CombinedKey<KO, K>, V> prefixValueGetter;
        private final byte[] negativeOneLong = Serdes.Long().serializer().serialize("fakeTopic", -1L);

        public KTableKTableJoinProcessor(final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, V> valueGetter) {
            this.prefixValueGetter = valueGetter.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            prefixValueGetter.init(context);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final KO key, final Change<VO> change) {
            if (key == null)
                throw new StreamsException("Record key for KTable foreignKeyJoin operator should not be null.");

            //Wrap it in a combinedKey and let the serializer handle the prefixing.
            final CombinedKey<KO, K> prefixKey = new CombinedKey<>(key);

            //Flush the foreign state store, as we need all elements to be flushed for a proper prefix scan.
            ref.flush();
            final KeyValueIterator<CombinedKey<KO, K>, V> prefixScanResults = prefixValueGetter.prefixScan(prefixKey);

            while (prefixScanResults.hasNext()) {

                final KeyValue<CombinedKey<KO, K>, V> scanResult = prefixScanResults.next();
                final K realKey = scanResult.key.getPrimaryKey();
                final V value2 = scanResult.value;
                VR newValue = null;

                if (change.newValue != null) {
                    newValue = joiner.apply(value2, change.newValue);
                }
                //Using -1 because we will not have race conditions from this side of the join to disambiguate with source OFFSET.
                context().headers().remove(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString());
                context().headers().add(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString(), negativeOneLong);
                context().forward(realKey, newValue);
            }
        }
    }
}
