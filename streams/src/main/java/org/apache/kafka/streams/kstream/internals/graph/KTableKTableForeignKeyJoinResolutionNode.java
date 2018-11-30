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
import org.apache.kafka.streams.kstream.internals.KTableValueGetterSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeyByPrimaryKeyPartitioner;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySerde;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Too much specific information to generalize so the Foreign Key KTable-KTable join requires a specific node.
 */
public class KTableKTableForeignKeyJoinResolutionNode<VR, K, V, KO, VO> extends StreamsGraphNode {
    private final ProcessorParameters<CombinedKey<KO, K>, V> joinOneToOneProcessorParameters;
    private final ProcessorParameters<KO, VO> joinByPrefixProcessorParameters;
    private final ProcessorParameters<CombinedKey<KO, K>, VR> resolverProcessorParameters;
    private final String finalRepartitionTopicName;
    private final String finalRepartitionSinkName;
    private final String finalRepartitionSourceName;
    private final CombinedKeySerde<KO, K> combinedKeySerde;
    private final Serde<VR> joinedValueSerde;
    private final KTableValueGetterSupplier<K, V> originalValueGetter;

    public KTableKTableForeignKeyJoinResolutionNode(final String nodeName,
                                                    final ProcessorParameters<CombinedKey<KO, K>, V> joinOneToOneProcessorParameters,
                                                    final ProcessorParameters<KO, VO> joinByPrefixProcessorParameters,
                                                    final ProcessorParameters<CombinedKey<KO, K>, VR> resolverProcessorParameters,
                                                    final String finalRepartitionTopicName,
                                                    final String finalRepartitionSinkName,
                                                    final String finalRepartitionSourceName,
                                                    final CombinedKeySerde<KO, K> combinedKeySerde,
                                                    final Serde<VR> joinedValueSerde,
                                                    final KTableValueGetterSupplier<K, V> originalValueGetter
    ) {
        super(nodeName, false);
        this.joinOneToOneProcessorParameters = joinOneToOneProcessorParameters;
        this.joinByPrefixProcessorParameters = joinByPrefixProcessorParameters;
        this.resolverProcessorParameters = resolverProcessorParameters;
        this.finalRepartitionTopicName = finalRepartitionTopicName;
        this.finalRepartitionSinkName = finalRepartitionSinkName;
        this.finalRepartitionSourceName = finalRepartitionSourceName;
        this.combinedKeySerde = combinedKeySerde;
        this.joinedValueSerde = joinedValueSerde;
        this.originalValueGetter = originalValueGetter;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addInternalTopic(finalRepartitionTopicName);
        //Repartition back to the original partitioning structure
        topologyBuilder.addSink(finalRepartitionSinkName, finalRepartitionTopicName,
                combinedKeySerde.serializer(), joinedValueSerde.serializer(),
                new CombinedKeyByPrimaryKeyPartitioner<>(this.combinedKeySerde, finalRepartitionTopicName),
                joinByPrefixProcessorParameters.processorName(), joinOneToOneProcessorParameters.processorName());

        topologyBuilder.addSource(null, finalRepartitionSourceName, new FailOnInvalidTimestamp(),
                combinedKeySerde.deserializer(), joinedValueSerde.deserializer(), finalRepartitionTopicName);

        //Connect highwaterProcessor to source, add the state store, and connect the statestore with the processor.
        topologyBuilder.addProcessor(resolverProcessorParameters.processorName(), resolverProcessorParameters.processorSupplier(), finalRepartitionSourceName);
        topologyBuilder.connectProcessorAndStateStores(resolverProcessorParameters.processorName(), originalValueGetter.storeNames());
    }
}
