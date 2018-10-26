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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.KeyValueStoreMaterializer;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Too much specific information to generalize so the Foreign Key KTable-KTable join requires a specific node.
 */
public class KTableKTableForeignKeyJoinResolutionNode<VR, K, KO, VO> extends StreamsGraphNode {
    private final String finalRepartitionSourceName;
    private final ProcessorParameters<K, Change<VR>> highwaterProcessorParameters;
    private final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal;
    private final String finalRepartitionTableName;
    private final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized;
    private final ProcessorParameters<K, VR> outputProcessorParameters;

    public KTableKTableForeignKeyJoinResolutionNode(final String nodeName,
                                                    final String finalRepartitionSourceName,
                                                    final ProcessorParameters<K, Change<VR>> highwaterProcessorParameters,
                                                    final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal,
                                                    final String finalRepartitionTableName,
                                                    final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                                    final ProcessorParameters<K, VR> outputProcessorParameters
    ) {
        super(nodeName, false);
        this.finalRepartitionSourceName = finalRepartitionSourceName;
        this.highwaterProcessorParameters = highwaterProcessorParameters;
        this.highwaterMatInternal = highwaterMatInternal;
        this.finalRepartitionTableName = finalRepartitionTableName;
        this.materialized = materialized;
        this.outputProcessorParameters = outputProcessorParameters;
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        //Connect highwaterProcessor to source, add the state store, and connect the statestore with the processor.
        topologyBuilder.addProcessor(highwaterProcessorParameters.processorName(), highwaterProcessorParameters.processorSupplier(), finalRepartitionSourceName);
        topologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(highwaterMatInternal).materialize(), highwaterProcessorParameters.processorName());
        topologyBuilder.connectProcessorAndStateStores(highwaterProcessorParameters.processorName(), finalRepartitionTableName);

        //Hook up the highwatermark output to KTableSource Processor
        topologyBuilder.addProcessor(outputProcessorParameters.processorName(), outputProcessorParameters.processorSupplier(), highwaterProcessorParameters.processorName());

        final StoreBuilder<KeyValueStore<K, VR>> storeBuilder
                = new KeyValueStoreMaterializer<>(materialized).materialize();
        topologyBuilder.addStateStore(storeBuilder, outputProcessorParameters.processorName());
        topologyBuilder.connectProcessorAndStateStores(outputProcessorParameters.processorName(), storeBuilder.name());
    }
}
