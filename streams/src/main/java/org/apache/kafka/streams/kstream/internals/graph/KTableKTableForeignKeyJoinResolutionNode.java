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
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Too much specific information to generalize so the Foreign Key KTable-KTable join requires a specific node.
 */
public class KTableKTableForeignKeyJoinResolutionNode<VR, K, V, KO, VO> extends StreamsGraphNode {
    private final ProcessorParameters<CombinedKey<KO, K>, V> joinOneToOneProcessorParameters;
    private final ProcessorParameters<KO, VO> joinByPrefixProcessorParameters;
    private final ProcessorParameters<K, VR> highwaterProcessorParameters;
    private final String finalRepartitionTopicName;
    private final String finalRepartitionSinkName;
    private final String finalRepartitionSourceName;
    private final Serde<K> thisKeySerde;
    private final Serde<VR> joinedValueSerde;
    private final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal;
    private final String finalRepartitionTableName;

    public KTableKTableForeignKeyJoinResolutionNode(final String nodeName,
                                                    final ProcessorParameters<CombinedKey<KO, K>, V> joinOneToOneProcessorParameters,
                                                    final ProcessorParameters<KO, VO> joinByPrefixProcessorParameters,
                                                    final ProcessorParameters<K, VR> highwaterProcessorParameters,
                                                    final String finalRepartitionTopicName,
                                                    final String finalRepartitionSinkName,
                                                    final String finalRepartitionSourceName,
                                                    final Serde<K> thisKeySerde,
                                                    final Serde<VR> joinedValueSerde,
                                                    final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal,
                                                    final String finalRepartitionTableName
    ) {
        super(nodeName, false);
        this.joinOneToOneProcessorParameters = joinOneToOneProcessorParameters;
        this.joinByPrefixProcessorParameters = joinByPrefixProcessorParameters;
        this.highwaterProcessorParameters = highwaterProcessorParameters;
        this.finalRepartitionTopicName = finalRepartitionTopicName;
        this.finalRepartitionSinkName = finalRepartitionSinkName;
        this.finalRepartitionSourceName = finalRepartitionSourceName;
        this.thisKeySerde = thisKeySerde;
        this.joinedValueSerde = joinedValueSerde;
        this.finalRepartitionTableName = finalRepartitionTableName;
        this.highwaterMatInternal = highwaterMatInternal;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        topologyBuilder.addInternalTopic(finalRepartitionTopicName);
        //Repartition back to the original partitioning structure
        topologyBuilder.addSink(finalRepartitionSinkName, finalRepartitionTopicName,
                thisKeySerde.serializer(), joinedValueSerde.serializer(),
                null,
                joinByPrefixProcessorParameters.processorName(), joinOneToOneProcessorParameters.processorName());

        topologyBuilder.addSource(null, finalRepartitionSourceName, new FailOnInvalidTimestamp(),
                thisKeySerde.deserializer(), joinedValueSerde.deserializer(), finalRepartitionTopicName);

        //Connect highwaterProcessor to source, add the state store, and connect the statestore with the processor.
        topologyBuilder.addProcessor(highwaterProcessorParameters.processorName(), highwaterProcessorParameters.processorSupplier(), finalRepartitionSourceName);
        topologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(highwaterMatInternal).materialize(), highwaterProcessorParameters.processorName());
        topologyBuilder.connectProcessorAndStateStores(highwaterProcessorParameters.processorName(), finalRepartitionTableName);
    }
}
