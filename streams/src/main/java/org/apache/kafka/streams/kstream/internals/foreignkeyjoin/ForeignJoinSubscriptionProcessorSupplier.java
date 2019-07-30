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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForeignJoinSubscriptionProcessorSupplier<K, KO, VO> implements ProcessorSupplier<KO, Change<VO>> {
    private static final Logger LOG = LoggerFactory.getLogger(ForeignJoinSubscriptionProcessorSupplier.class);
    private final StoreBuilder<TimestampedKeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper<K>>> storeBuilder;

    public ForeignJoinSubscriptionProcessorSupplier(
        final StoreBuilder<TimestampedKeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper<K>>> storeBuilder) {

        this.storeBuilder = storeBuilder;
    }

    @Override
    public Processor<KO, Change<VO>> get() {
        return new KTableKTableJoinProcessor();
    }


    private final class KTableKTableJoinProcessor extends AbstractProcessor<KO, Change<VO>> {
        private Sensor skippedRecordsSensor;
        private TimestampedKeyValueStore<CombinedKey<KO, K>, SubscriptionWrapper<K>> store;

        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            final InternalProcessorContext internalProcessorContext = (InternalProcessorContext) context;
            skippedRecordsSensor = ThreadMetrics.skipRecordSensor(internalProcessorContext.metrics());
            store = internalProcessorContext.getStateStore(storeBuilder);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final KO key, final Change<VO> value) {
            // if the key is null, we do not need proceed aggregating
            // the record with the table
            if (key == null) {
                LOG.warn(
                        "Skipping record due to null key. value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                        value, context().topic(), context().partition(), context().offset()
                );
                skippedRecordsSensor.record();
                return;
            }

            //Make a CombinedKey that contains ONLY the prefix and no PK. When serialized, it will allow the prefixScan
            //to extract serialized CombinedKeys containing both the PK and FK.
            final CombinedKey<KO, K> prefixKey = new CombinedKey<>(key);

            //Perform the prefixScan and propagate the results
            try (final KeyValueIterator<CombinedKey<KO, K>, ValueAndTimestamp<SubscriptionWrapper<K>>> prefixScanResults = store.prefixScan(prefixKey)) {
                while (prefixScanResults.hasNext()) {
                    final KeyValue<CombinedKey<KO, K>, ValueAndTimestamp<SubscriptionWrapper<K>>> scanResult = prefixScanResults.next();
                    context().forward(scanResult.key.getPrimaryKey(), new SubscriptionResponseWrapper<>(scanResult.value.value().getHash(), value.newValue));
                }
            }
        }
    }
}
