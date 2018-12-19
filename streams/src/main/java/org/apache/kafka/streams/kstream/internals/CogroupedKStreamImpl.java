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
package org.apache.kafka.streams.kstream.internals;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.internals.graph.CogroupedKTableNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

class CogroupedKStreamImpl<K, V> implements CogroupedKStream<K, V> {
    private final AtomicInteger index = new AtomicInteger(0);
    private static final String COGROUP_AGGREGATE_NAME = "KSTREAM-COGROUP-AGGREGATE-";
    private static final String COGROUP_NAME = "KSTREAM-COGROUP-";
    private enum AggregateType {
        AGGREGATE,
        SESSION_WINDOW_AGGREGATE,
        WINDOW_AGGREGATE
    }
    private final InternalStreamsBuilder builder;
    private final Serde<K> keySerde;
    private final Map<KGroupedStream, Aggregator> pairs = new HashMap<>();
    private final Map<KGroupedStreamImpl, String> repartitionNames = new HashMap<>();
    //private final CogroupedKStreamAggregateBuilder<K, V> aggregateBuilder;

    <T> CogroupedKStreamImpl(final InternalStreamsBuilder builder,
                             final KGroupedStream<K, T> groupedStream,
                             final Serde<K> keySerde,
                             final Aggregator<? super K, ? super T, V> aggregator) {
        this.builder = builder;
        this.keySerde = keySerde;
        cogroup(groupedStream, aggregator);
    }

    @Override
    public <T> CogroupedKStream<K, V> cogroup(final KGroupedStream<K, T> groupedStream,
                                              final Aggregator<? super K, ? super T, V> aggregator) {
        Objects.requireNonNull(groupedStream, "groupedStream can't be null");
        Objects.requireNonNull(aggregator, "aggregator can't be null");
        pairs.put(groupedStream, aggregator);
        return this;
    }

    @Override
    public <VR> KTable<K, VR> aggregate(Initializer<VR> initializer, Serde<VR> valueSerde) {
        return aggregate(initializer, valueSerde, Materialized.with(keySerde, null));
    }

    @Override
    public <VR> KTable<K, VR> aggregate(Initializer<VR> initializer, Serde<VR> valueSerde, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(valueSerde, "valueSerde can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, COGROUP_AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }

        return doAggregate(
                initializer,
                valueSerde,
                materializedInternal
        );
    }

    @SuppressWarnings("unchecked")
    private <VR> KTable<K, VR> doAggregate(final Initializer<VR> initializer,
                                           final Serde<VR> valSerde,
                                           final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal) {

        final StoreBuilder<KeyValueStore<K, V>> kvsm = new KeyValueStoreMaterializer(materializedInternal).materialize();
        builder.addStateStore(kvsm);

        final Set<String> sourceNodes = new HashSet<>();
        final Collection<KStreamAggProcessorSupplier> processors = new ArrayList<>();
        final Collection<StreamsGraphNode> parentNodes = new ArrayList<>();

        final List<String> processorNames = new ArrayList<>();
        for (final Map.Entry<KGroupedStream, Aggregator> pair : pairs.entrySet()) {
            final KGroupedStreamImpl groupedStream = (KGroupedStreamImpl) pair.getKey();

            final String aggFunctionName = builder.newProcessorName(COGROUP_AGGREGATE_NAME);

            final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();

            final String sourceName = repartitionIfRequired(groupedStream, repartitionNodeBuilder);

            StreamsGraphNode parentNode = groupedStream.streamsGraphNode;

            if (!sourceName.equals(groupedStream.name)) {
                final StreamsGraphNode repartitionNode = repartitionNodeBuilder.build();
                builder.addGraphNode(parentNode, repartitionNode);
                parentNode = repartitionNode;
            }

            final KStreamAggProcessorSupplier processor = new KStreamAggregate(materializedInternal.storeName(), initializer, pair.getValue());

            processorNames.add(aggFunctionName);

            final StatefulProcessorNode<K, V> statefulProcessorNode =
                    new StatefulProcessorNode<>(
                            aggFunctionName,
                            new ProcessorParameters<>(processor, aggFunctionName),
                            new String[]{kvsm.name()},
                            groupedStream.isRepartitionRequired()
                    );

            builder.addGraphNode(parentNode, statefulProcessorNode);

            if (sourceName.equals(groupedStream.name)) {
                sourceNodes.addAll(groupedStream.sourceNodes);
            } else {
                sourceNodes.add(sourceName);
            }
            parentNodes.add(statefulProcessorNode);
            processors.add(processor);
        }


//                  case SESSION_WINDOW_AGGREGATE:
//                    processor = new KStreamSessionWindowAggregate(sessionWindows, storeSupplier.name(), initializer, pair.getValue(), sessionMerger);
//                    break;
//                case WINDOW_AGGREGATE:
//                    processor = new KStreamWindowAggregate(windows, storeSupplier.name(), initializer, pair.getValue());
//                    break;
//                default:
//                    throw new IllegalStateException("Unrecognized AggregateType.");
//            }
//        }
        final String name = newName(COGROUP_NAME);
        final KStreamCogroupProcessorSupplier cogroup = new KStreamCogroupProcessorSupplier(processors);
        StreamsGraphNode streamsGraphNode = new CogroupedKTableNode(name, false, cogroup, processorNames, sourceNodes);

        //TODO - Fix the String type to be actual parent type?
        builder.addGraphNode(parentNodes, streamsGraphNode);

        return new KTableImpl<K, String, VR>(name, keySerde, valSerde, sourceNodes, kvsm.name(), true, cogroup, streamsGraphNode, builder);

    }

    private String newName(String prefix) {
        return prefix + String.format("%010d", index.getAndIncrement());
    }



    /**
     * @return the new sourceName if repartitioned. Otherwise the name of this stream
     */
    String repartitionIfRequired(final KGroupedStreamImpl groupedStream,
                         final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<K, V> optimizableRepartitionNodeBuilder) {
        if (!groupedStream.isRepartitionRequired()) {
            return groupedStream.name;
        }
        // if repartition required the operation
        // captured needs to be set in the graph
        return KStreamImpl.createRepartitionedSource(builder, keySerde, groupedStream.valSerde, groupedStream.name, optimizableRepartitionNodeBuilder);
    }

}