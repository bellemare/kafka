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

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.Arrays;

public class TableProcessorNode<K, V> extends StreamsGraphNode {

    private final ProcessorParameters<K, V> processorParameters;
    private final StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder;
    private final String[] storeNames;

    public TableProcessorNode(final String nodeName,
                              final ProcessorParameters<K, V> processorParameters,
                              final StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder) {
        this(nodeName, processorParameters, storeBuilder, null);
    }

    public TableProcessorNode(final String nodeName,
                              final ProcessorParameters<K, V> processorParameters,
                              final StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder,
                              final String[] storeNames) {
        super(nodeName);
        this.processorParameters = processorParameters;
        this.storeBuilder = storeBuilder;
        this.storeNames = storeNames != null ? storeNames : new String[] {};
    }

    @Override
    public String toString() {
        return "TableProcessorNode{" +
            ", processorParameters=" + processorParameters +
            ", storeBuilder=" + (storeBuilder == null ? "null" : storeBuilder.name()) +
            ", storeNames=" + Arrays.toString(storeNames) +
            "} " + super.toString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final String processorName = processorParameters.processorName();
        topologyBuilder.addProcessor(processorName, processorParameters.processorSupplier(), parentNodeNames());

//<<<<<<< b86e8c1ea908b8fa2576d058700b0ac14b1b4a3e
        if (storeNames.length > 0) {
            topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
        }

        // TODO: we are enforcing this as a keyvalue store, but it should go beyond any type of stores
        if (storeBuilder != null) {
            topologyBuilder.addStateStore(storeBuilder, processorName);
//=======
//        if (shouldMaterialize) {
//            topologyBuilder.addStateStore(new KeyValueStoreMaterializer<>((MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>>) materializedInternal).materialize(), processorName);
//>>>>>>> KAFKA-3705 - Added a many to one foreign key joiner, as per KIP-213
        }

        if (storeNames.length > 0) {
            topologyBuilder.connectProcessorAndStateStores(processorName, storeNames);
        }
    }
}
