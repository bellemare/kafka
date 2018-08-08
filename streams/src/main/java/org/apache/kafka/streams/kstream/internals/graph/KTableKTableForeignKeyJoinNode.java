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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KTableImpl;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.onetomany.ChangedSerde;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKey;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKeyLeftKeyPartitioner;
import org.apache.kafka.streams.kstream.internals.onetomany.PropagationWrapper;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Too much specific information to generalize so the Foreign Key KTable-KTable join requires a specific node.
 */
public class KTableKTableForeignKeyJoinNode<V0, KL, VL, KR, VR> extends StreamsGraphNode {

    private String repartitionTopicName;
    private String repartitionSinkName;
    private String repartitionSourceName;
    private ProcessorParameters repartitionProcessorParameters;
    private ProcessorParameters joinOneToOneProcessorParameters;
    private Serde<CombinedKey<KR, KL>> combinedKeySerde;
    private Serde<PropagationWrapper<VL>> propagationWrapperSerde;
    private CombinedKeyLeftKeyPartitioner<KR, KL, PropagationWrapper<VL>> partitioner;

    private MaterializedInternal<CombinedKey<KR,KL>, VL, KeyValueStore<Bytes, byte[]>> repartitionedRangeScannableStore;
    private ProcessorParameters<KR, VR> joinByPrefixProcessorParameters;

    private String finalRepartitionTopicName;
    private String finalRepartitionSinkName;
    private String finalRepartitionSourceName;
    private Serde<KL> thisKeySerde;
    private ChangedSerde<Change<PropagationWrapper<V0>>> changedSerde;

    private ProcessorParameters<KL, PropagationWrapper<V0>> highwaterProcessorParameters;
    private MaterializedInternal<KL, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal;
    private String finalRepartitionTableName;
    private String rangeScannableDBRefName;

    private MaterializedInternal<KL, V0, KeyValueStore<Bytes, byte[]>> materialized;
    private ProcessorParameters<KL, V0> outputProcessorParameters;
    private final String thisName;
    private final String otherName;
    private final String[] otherValueGetterStoreNames;


    public KTableKTableForeignKeyJoinNode(final String nodeName,
                                   final String repartitionTopicName,
                                   final String repartitionSinkName,
                                   final String repartitionSourceName,
                                   final ProcessorParameters repartitionProcessorParameters,
                                   final ProcessorParameters joinOneToOneProcessorParameters,
                                   final Serde<CombinedKey<KR, KL>> combinedKeySerde,
                                   final Serde<PropagationWrapper<VL>> propagationWrapperSerde,
                                   final CombinedKeyLeftKeyPartitioner<KR, KL, PropagationWrapper<VL>> partitioner,
                                   final MaterializedInternal<CombinedKey<KR,KL>, VL, KeyValueStore<Bytes, byte[]>> repartitionedRangeScannableStore,
                                   final ProcessorParameters<KR, VR> joinByPrefixProcessorParameters,
                                   final String finalRepartitionTopicName,
                                   final String finalRepartitionSinkName,
                                   final String finalRepartitionSourceName,
                                   final Serde<KL> thisKeySerde,
                                   final ChangedSerde<Change<PropagationWrapper<V0>>> changedSerde,
                                   final ProcessorParameters<KL, PropagationWrapper<V0>> highwaterProcessorParameters,
                                   final MaterializedInternal<KL, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal,
                                   final String finalRepartitionTableName,
                                   final String rangeScannableDBRefName,
                                   final MaterializedInternal<KL, V0, KeyValueStore<Bytes, byte[]>> materialized,
                                   final ProcessorParameters<KL, V0> outputProcessorParameters,
                                   final String thisName,
                                   final String otherName,
                                   final String[] otherValueGetterStoreNames
                                   ) {
        super(nodeName, false);
        this.repartitionTopicName = repartitionTopicName;
        this.repartitionSinkName = repartitionSinkName;
        this.repartitionSourceName = repartitionSourceName;
        this.repartitionProcessorParameters = repartitionProcessorParameters;
        this.joinOneToOneProcessorParameters = joinOneToOneProcessorParameters;
        this.combinedKeySerde = combinedKeySerde;
        this.propagationWrapperSerde = propagationWrapperSerde;
        this.partitioner = partitioner;
        this.repartitionedRangeScannableStore = repartitionedRangeScannableStore;
        this.joinByPrefixProcessorParameters = joinByPrefixProcessorParameters;
        this.finalRepartitionTopicName = finalRepartitionTopicName;
        this.finalRepartitionSinkName = finalRepartitionSinkName;
        this.finalRepartitionSourceName = finalRepartitionSourceName;
        this.thisKeySerde = thisKeySerde;
        this.changedSerde = changedSerde;
        this.highwaterProcessorParameters = highwaterProcessorParameters;
        this.highwaterMatInternal = highwaterMatInternal;
        this.finalRepartitionTableName = finalRepartitionTableName;
        this.rangeScannableDBRefName = rangeScannableDBRefName;
        this.materialized = materialized;
        this.outputProcessorParameters = outputProcessorParameters;
        this.thisName = thisName;
        this.otherName = otherName;
        this.otherValueGetterStoreNames = otherValueGetterStoreNames;

    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
//        final String thisProcessorName = thisProcessorParameters().processorName();
//        final String otherProcessorName = otherProcessorParameters().processorName();
//        final String mergeProcessorName = mergeProcessorParameters().processorName();
//
//        topologyBuilder.addProcessor(thisProcessorName,
//                                     thisProcessorParameters().processorSupplier(),
//                                     thisJoinSideNodeName());
//
//        topologyBuilder.addProcessor(otherProcessorName,
//                                     otherProcessorParameters().processorSupplier(),
//                                     otherJoinSideNodeName());
//
//        topologyBuilder.addProcessor(mergeProcessorName,
//                                     mergeProcessorParameters().processorSupplier(),
//                                     thisProcessorName,
//                                     otherProcessorName);
//
//        topologyBuilder.connectProcessorAndStateStores(thisProcessorName,
//                                                       joinOtherStoreNames);
//        topologyBuilder.connectProcessorAndStateStores(otherProcessorName,
//                                                       joinThisStoreNames);
//
//        if (materializedInternal != null) {
//            final StoreBuilder<KeyValueStore<K, VR>> storeBuilder
//                = new KeyValueStoreMaterializer<>(materializedInternal).materialize();
//            topologyBuilder.addStateStore(storeBuilder, mergeProcessorName);
//        }

        topologyBuilder.addInternalTopic(repartitionTopicName);
        topologyBuilder.addProcessor(repartitionProcessorParameters.processorName(),
                                     repartitionProcessorParameters.processorSupplier(),
                thisName);
        //Repartition to the KR prefix of the CombinedKey.
        topologyBuilder.addSink(repartitionSinkName, repartitionTopicName,
                combinedKeySerde.serializer(), propagationWrapperSerde.serializer(),
                partitioner, repartitionProcessorParameters.processorName());

        topologyBuilder.addSource(null, repartitionSourceName, new FailOnInvalidTimestamp(),
                combinedKeySerde.deserializer(), propagationWrapperSerde.deserializer(), repartitionTopicName);

        topologyBuilder.addProcessor(joinOneToOneProcessorParameters.processorName(),
                                     joinOneToOneProcessorParameters.processorSupplier(),
                                     repartitionSourceName);

        //Connect the left processor with the state store.
        topologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(repartitionedRangeScannableStore).materialize(), joinOneToOneProcessorParameters.processorName());
        //Add the right processor to the topology.
        topologyBuilder.addProcessor(joinByPrefixProcessorParameters.processorName(), joinByPrefixProcessorParameters.processorSupplier(), otherName);

        topologyBuilder.addInternalTopic(finalRepartitionTopicName);
        //Repartition back to the original partitioning structure
        topologyBuilder.addSink(finalRepartitionSinkName, finalRepartitionTopicName,
                thisKeySerde.serializer(), changedSerde.serializer(),
                null,
                joinByPrefixProcessorParameters.processorName(), joinOneToOneProcessorParameters.processorName());
        topologyBuilder.addSource(null, finalRepartitionSourceName, new FailOnInvalidTimestamp(),
                thisKeySerde.deserializer(), changedSerde.deserializer(), finalRepartitionTopicName);

        //Connect highwaterProcessor to source, add the state store, and connect the statestore with the processor.
        topologyBuilder.addProcessor(highwaterProcessorParameters.processorName(), highwaterProcessorParameters.processorSupplier(), finalRepartitionSourceName);
        topologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(highwaterMatInternal).materialize(), highwaterProcessorParameters.processorName());
        topologyBuilder.connectProcessorAndStateStores(highwaterProcessorParameters.processorName(), finalRepartitionTableName);
        //Connect the first-stage processors to the source state stores.
        topologyBuilder.connectProcessorAndStateStores(joinByPrefixProcessorParameters.processorName(), rangeScannableDBRefName);
        topologyBuilder.connectProcessorAndStateStores(joinOneToOneProcessorParameters.processorName(), otherValueGetterStoreNames);

        //TODO - Remedy the this and other references above.

//TODO - Don't forget to add this back!
//        KTableSource outputProcessor = new KTableSource<K, V0>(materialized.storeName());
//        final String outputProcessorName = builder.newProcessorName(SOURCE_NAME);
        //Hook up the highwatermark output to KTableSource Processor
        topologyBuilder.addProcessor(outputProcessorParameters.processorName(), outputProcessorParameters.processorSupplier(), highwaterProcessorParameters.processorName());

        final StoreBuilder<KeyValueStore<KL, V0>> storeBuilder
                = new KeyValueStoreMaterializer<>(materialized).materialize();
        topologyBuilder.addStateStore(storeBuilder, outputProcessorParameters.processorName());
        topologyBuilder.connectProcessorAndStateStores(outputProcessorParameters.processorName(), storeBuilder.name());








    }

    @Override
    public String toString() {
        //TODO - Bellemare
        return "KTableKTableJoinNode{";
    }

//    public static <V0, KL, VL, KR, VR> KTableKTableJoinNodeBuilder<V0, KL, VL, KR, VR> kTableKTableJoinNodeBuilder() {
//        return new KTableKTableJoinNodeBuilder<>();
//    }
//
//    public static final class KTableKTableJoinNodeBuilder<V0, KL, VL, KR, VR> {
//
//        private String repartitionTopicName;
//        private String repartitionSinkName;
//        private String repartitionSourceName;
//        private ProcessorParameters repartitionProcessorParameters;
//        private ProcessorParameters joinOneToOneProcessorParameters;
//        private Serde<CombinedKey<KR, KL>> combinedKeySerde;
//        private Serde<PropagationWrapper<VL>> propagationWrapperSerde;
//        private CombinedKeyLeftKeyPartitioner<KR, KL, PropagationWrapper<VL>> partitioner;
//
//        private MaterializedInternal<CombinedKey<KR,KL>, VL, KeyValueStore<Bytes, byte[]>> repartitionedRangeScannableStore;
//        private ProcessorParameters<KR, VR> joinByPrefixProcessorParameters;
//
//        private String finalRepartitionTopicName;
//        private String finalRepartitionSinkName;
//        private String finalRepartitionSourceName;
//        private Serde<KL> thisKeySerde;
//        private ChangedSerde<Change<PropagationWrapper<V0>>> changedSerde;
//
//        private ProcessorParameters<KL, PropagationWrapper<V0>> highwaterProcessorParameters;
//        private MaterializedInternal<KL, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal;
//        private String finalRepartitionTableName;
//        private String rangeScannableDBRefName;
//
//        private MaterializedInternal<KL, V0, KeyValueStore<Bytes, byte[]>> materialized;
//        private ProcessorParameters<KL, V0> outputProcessorParameters;
//
//        private KTableKTableJoinNodeBuilder() {
//        }
//
//        public KTableKTableForeignKeyJoinNode<V0, KL, VL, KR, VR> build() {
//
//            return new KTableKTableForeignKeyJoinNode<V0, KL, VL, KR, VR>("nodename",
//                    repartitionTopicName,
//                    repartitionSinkName,
//                    repartitionSourceName,
//                    repartitionProcessorParameters,
//                    joinOneToOneProcessorParameters,
//                    combinedKeySerde,
//                    propagationWrapperSerde,
//                    partitioner,
//                    repartitionedRangeScannableStore,
//                    joinByPrefixProcessorParameters,
//                    finalRepartitionTopicName,
//                    finalRepartitionSinkName,
//                    finalRepartitionSourceName,
//                    thisKeySerde,
//                    changedSerde,
//                    highwaterProcessorParameters,
//                    highwaterMatInternal,
//                    finalRepartitionTableName,
//                    rangeScannableDBRefName,
//                    materialized,
//                    outputProcessorParameters);
//        }
//    }
}
