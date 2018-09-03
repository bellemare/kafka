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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import static org.apache.kafka.streams.kstream.internals.foreignkeyjoin.UserRecordHeaderIsolatorUtil.addUserHeaderPrefix;

public class KTableRepartitionerProcessorSupplier<K, KO, V> implements ProcessorSupplier<K, Change<V>> {

    private final ValueMapper<V, KO> mapper;
    private final byte[] falseByteArray = {(byte) 0};
    private final byte[] trueByteArray = {(byte) 1};

    public KTableRepartitionerProcessorSupplier(final ValueMapper<V, KO> extractor) {
        this.mapper = extractor;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new UnbindChangeProcessor();
    }

    private class UnbindChangeProcessor extends AbstractProcessor<K, Change<V>> {
        private final Serializer<Long> longSerializer = Serdes.Long().serializer();

        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
        }

        @Override
        public void process(final K key, final Change<V> change) {
            addUserHeaderPrefix(context());
            if (change.oldValue != null) {
                final KO oldForeignKey = mapper.apply(change.oldValue);
                final CombinedKey<KO, K> combinedOldKey = new CombinedKey<>(oldForeignKey, key);
                if (change.newValue != null) {
                    final KO extractedNewForeignKey = mapper.apply(change.newValue);
                    final CombinedKey<KO, K> combinedNewKey = new CombinedKey<>(extractedNewForeignKey, key);

                    //Requires equal to be defined...
                    if (oldForeignKey.equals(extractedNewForeignKey)) {
                        //Same foreign key. Just propagate onwards.
                        final byte[] offset = longSerializer.serialize(null, context().offset());
                        context().headers().add(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString(), offset);
                        context().headers().add(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString(), trueByteArray);
                        context().forward(combinedNewKey, change.newValue);
                    } else {
                        //Different Foreign Key - delete the old key value and propagate the new one.
                        //Note that we indicate that we don't want to propagate the delete to the join output. It is set to false.
                        //This will be used by a downstream processor to delete it from the local state store, but not propagate it
                        //as a full delete. This avoids a race condition in the resolution of the output.
                        final byte[] offset = longSerializer.serialize(null, context().offset());
                        context().headers().add(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString(), offset);
                        context().headers().add(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString(), falseByteArray);
                        //Don't need to send a value, as this only indicates a delete.
                        context().forward(combinedOldKey, null);

                        context().headers().remove(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString());
                        context().headers().add(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString(), trueByteArray);
                        context().forward(combinedNewKey, change.newValue);
                    }
                } else {
                    final byte[] offset = longSerializer.serialize(null, context().offset());
                    context().headers().add(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString(), offset);
                    context().headers().add(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString(), trueByteArray);
                    context().forward(combinedOldKey, null);
                }
            } else if (change.newValue != null) {
                final KO extractedForeignKeyValue = mapper.apply(change.newValue);
                final CombinedKey<KO, K> newCombinedKeyValue = new CombinedKey<>(extractedForeignKeyValue, key);
                final byte[] offset = longSerializer.serialize(null, context().offset());
                context().headers().add(ForeignKeyJoinInternalHeaderTypes.OFFSET.toString(), offset);
                context().headers().add(ForeignKeyJoinInternalHeaderTypes.PROPAGATE.toString(), trueByteArray);
                context().forward(newCombinedKeyValue, change.newValue);
            }
        }

        @Override
        public void close() {}
    }
}
