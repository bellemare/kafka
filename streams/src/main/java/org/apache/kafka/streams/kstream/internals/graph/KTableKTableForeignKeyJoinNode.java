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
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeyByForeignKeyPartitioner;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Too much specific information to generalize so the Foreign Key KTable-KTable join requires a specific node.
 */
public class KTableKTableForeignKeyJoinNode<VR, K, V, KO, VO> extends StreamsGraphNode {
    private final String repartitionSourceName;
    private final ProcessorParameters joinOneToOneProcessorParameters;
    private final Serde<CombinedKey<KO, K>> combinedKeySerde;
    private final Serde<V> thisValueSerde;
    private final MaterializedInternal<CombinedKey<KO, K>, V, KeyValueStore<Bytes, byte[]>> repartitionedPrefixScannableStore;
    private final ProcessorParameters<KO, Change<VO>> joinByPrefixProcessorParameters;
    private final String finalRepartitionTopicName;
    private final String finalRepartitionSinkName;
    private final Serde<K> thisKeySerde;
    private final Serde<VR> joinedValueSerde;
    private final String otherName;
    private final String prefixScannableDBRefName;
    private final String[] otherValueGetterStoreNames;

    //TODO - Can reduce some of the parameters, but < 13 is not possible at the moment.
    //Would likely need to split into two graphNodes - ie: foreignKeyJoinNode and foreignKeyJoinOrderResolutionNode.
    public KTableKTableForeignKeyJoinNode(final String nodeName,
//                                          final String repartitionTopicName,
//                                          final String repartitionSinkName,
                                          final String repartitionSourceName,
//                                          final ProcessorParameters repartitionProcessorParameters,
                                          final ProcessorParameters joinOneToOneProcessorParameters,
                                          final Serde<CombinedKey<KO, K>> combinedKeySerde,
                                          final Serde<V> thisValueSerde,
//                                          final CombinedKeyByForeignKeyPartitioner<KO, K, V> partitioner,
                                          final MaterializedInternal<CombinedKey<KO, K>, V, KeyValueStore<Bytes, byte[]>> repartitionedPrefixScannableStore,
                                          final ProcessorParameters<KO, Change<VO>> joinByPrefixProcessorParameters,
                                          final String finalRepartitionTopicName,
                                          final String finalRepartitionSinkName,
                                          final Serde<K> thisKeySerde,
                                          final Serde<VR> joinedValueSerde,
//                                          final String thisName,
                                          final String otherName,
                                          final String prefixScannableDBRefName,
                                          final String[] otherValueGetterStoreNames
    ) {
        super(nodeName, false);
//        this.repartitionTopicName = repartitionTopicName;
//        this.repartitionSinkName = repartitionSinkName;
        this.repartitionSourceName = repartitionSourceName;
//        this.repartitionProcessorParameters = repartitionProcessorParameters;
        this.joinOneToOneProcessorParameters = joinOneToOneProcessorParameters;
        this.combinedKeySerde = combinedKeySerde;
        this.thisValueSerde = thisValueSerde;
//        this.partitioner = partitioner;
        this.repartitionedPrefixScannableStore = repartitionedPrefixScannableStore;
        this.joinByPrefixProcessorParameters = joinByPrefixProcessorParameters;
        this.finalRepartitionTopicName = finalRepartitionTopicName;
        this.finalRepartitionSinkName = finalRepartitionSinkName;
        this.thisKeySerde = thisKeySerde;
        this.joinedValueSerde = joinedValueSerde;
//        this.thisName = thisName;
        this.otherName = otherName;
        this.prefixScannableDBRefName = prefixScannableDBRefName;
        this.otherValueGetterStoreNames = otherValueGetterStoreNames;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
//        topologyBuilder.addInternalTopic(repartitionTopicName);
//        topologyBuilder.addProcessor(repartitionProcessorParameters.processorName(),
//                repartitionProcessorParameters.processorSupplier(),
//                thisName);
//        //Repartition to the KR prefix of the CombinedKey.
//        topologyBuilder.addSink(repartitionSinkName, repartitionTopicName,
//                combinedKeySerde.serializer(), thisValueSerde.serializer(),
//                partitioner, repartitionProcessorParameters.processorName());
//
//        topologyBuilder.addSource(null, repartitionSourceName, new FailOnInvalidTimestamp(),
//                combinedKeySerde.deserializer(), thisValueSerde.deserializer(), repartitionTopicName);



        topologyBuilder.addProcessor(joinOneToOneProcessorParameters.processorName(),
                joinOneToOneProcessorParameters.processorSupplier(),
                repartitionSourceName);

        //Connect the left processor with the state store.
        topologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(repartitionedPrefixScannableStore).materialize(), joinOneToOneProcessorParameters.processorName());
        //Add the right processor to the topology.


        topologyBuilder.addProcessor(joinByPrefixProcessorParameters.processorName(), joinByPrefixProcessorParameters.processorSupplier(), otherName);

        //Connect the first-stage processors to the source state stores.
        topologyBuilder.connectProcessorAndStateStores(joinByPrefixProcessorParameters.processorName(), prefixScannableDBRefName);
        topologyBuilder.connectProcessorAndStateStores(joinOneToOneProcessorParameters.processorName(), otherValueGetterStoreNames);


        topologyBuilder.addInternalTopic(finalRepartitionTopicName);
        //Repartition back to the original partitioning structure
        topologyBuilder.addSink(finalRepartitionSinkName, finalRepartitionTopicName,
                thisKeySerde.serializer(), joinedValueSerde.serializer(),
                null,
                joinByPrefixProcessorParameters.processorName(), joinOneToOneProcessorParameters.processorName());
    }
}
