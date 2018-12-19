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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.SessionWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.graph.CogroupedKTableNode;
import org.apache.kafka.streams.kstream.internals.graph.OptimizableRepartitionNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StatefulProcessorNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionBytesStoreSupplier;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.SessionStoreBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl.AGGREGATE_NAME;

class SessionWindowedCogroupedKStreamImpl<K, V> implements SessionWindowedCogroupedKStream<K, V> {
    private final AtomicInteger index = new AtomicInteger(0);
    private final InternalStreamsBuilder builder;
    private final SessionWindows sessionWindows;
    private final Map<KGroupedStream, Aggregator> pairs;
    private final Serde<K> keySerde;
    private final String name;

    private static final String SESSION_COGROUP_AGGREGATE_NAME = "KSTREAM-COGROUP-SESSION-AGGREGATE-";
    private static final String SESSION_COGROUP_NAME = "KSTREAM-COGROUP-SESSION-";

    public SessionWindowedCogroupedKStreamImpl(SessionWindows sessionWindows,
                                               InternalStreamsBuilder builder,
                                               Map<KGroupedStream, Aggregator> pairs,
                                               String name,
                                               Serde<K> keySerde) {
        this.sessionWindows = sessionWindows;
        this.builder = builder;
        this.pairs = pairs;
        this.keySerde = keySerde;
        this.name = name;
    }

    @Override
    public <VR> KTable<Windowed<K>, VR> aggregate(final Initializer<VR> initializer,
                                                  final Merger<? super K, VR> sessionMerger,
                                                  final Materialized<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(initializer, "initializer can't be null");
        Objects.requireNonNull(sessionMerger, "sessionMerger can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, AGGREGATE_NAME);

        if (materializedInternal.keySerde() == null) {
            materializedInternal.withKeySerde(keySerde);
        }

        return doSessionAggregate(initializer,
                sessionMerger,
                materializedInternal.keySerde() != null ? new WindowedSerdes.SessionWindowedSerde<>(materializedInternal.keySerde()) : null,
                materializedInternal);
    }


    private <KR, VR> KTable<KR, VR> doSessionAggregate(final Initializer<VR> initializer,
                                                            final Merger<? super K, VR> sessionMerger,
                                                            final Serde<KR> someKeySerde,
                                                            final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materializedInternal) {


        final StoreBuilder<SessionStore<K, VR>> kvsm = materialize(materializedInternal);
        builder.addStateStore(kvsm);

        final Set<String> sourceNodes = new HashSet<>();
        final Collection<KStreamAggProcessorSupplier> processors = new ArrayList<>();
        final Collection<StreamsGraphNode> parentNodes = new ArrayList<>();

        final List<String> processorNames = new ArrayList<>();
        for (final Map.Entry<KGroupedStream, Aggregator> pair : pairs.entrySet()) {
            final KGroupedStreamImpl groupedStream = (KGroupedStreamImpl) pair.getKey();

            final String aggFunctionName = builder.newProcessorName(SESSION_COGROUP_AGGREGATE_NAME);

            final OptimizableRepartitionNode.OptimizableRepartitionNodeBuilder<K, V> repartitionNodeBuilder = OptimizableRepartitionNode.optimizableRepartitionNodeBuilder();

            final String sourceName = repartitionIfRequired(groupedStream, repartitionNodeBuilder);

            StreamsGraphNode parentNode = groupedStream.streamsGraphNode;

            if (!sourceName.equals(groupedStream.name)) {
                final StreamsGraphNode repartitionNode = repartitionNodeBuilder.build();
                builder.addGraphNode(parentNode, repartitionNode);
                parentNode = repartitionNode;
            }

            final KStreamAggProcessorSupplier processor = new KStreamSessionWindowAggregate<>(sessionWindows, materializedInternal.storeName(), initializer, pair.getValue(), sessionMerger);
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

        final String name = newName(SESSION_COGROUP_NAME);
        final KStreamCogroupProcessorSupplier cogroup = new KStreamCogroupProcessorSupplier(processors);
        StreamsGraphNode streamsGraphNode = new CogroupedKTableNode(name, false, cogroup, processorNames, sourceNodes);

        builder.addGraphNode(parentNodes, streamsGraphNode);
        return new KTableImpl<>(name, someKeySerde, materializedInternal.valueSerde(), sourceNodes, kvsm.name(), true, cogroup, streamsGraphNode, builder);
    }



    private <VR> StoreBuilder<SessionStore<K, VR>> materialize(final MaterializedInternal<K, VR, SessionStore<Bytes, byte[]>> materialized) {
        SessionBytesStoreSupplier supplier = (SessionBytesStoreSupplier) materialized.storeSupplier();
        if (supplier == null) {
            // NOTE: in the future, when we remove Windows#maintainMs(), we should set the default retention
            // to be (windows.inactivityGap() + windows.grace()). This will yield the same default behavior.
            final long retentionPeriod = materialized.retention() != null ? materialized.retention().toMillis() : sessionWindows.maintainMs();

            if ((sessionWindows.inactivityGap() + sessionWindows.gracePeriodMs()) > retentionPeriod) {
                throw new IllegalArgumentException("The retention period of the session store "
                        + materialized.storeName()
                        + " must be no smaller than the session inactivity gap plus the"
                        + " grace period."
                        + " Got gap=[" + sessionWindows.inactivityGap() + "],"
                        + " grace=[" + sessionWindows.gracePeriodMs() + "],"
                        + " retention=[" + retentionPeriod + "]");
            }
            supplier = Stores.persistentSessionStore(
                    materialized.storeName(),
                    retentionPeriod
            );
        }
        final StoreBuilder<SessionStore<K, VR>> builder = Stores.sessionStoreBuilder(
                supplier,
                materialized.keySerde(),
                materialized.valueSerde()
        );

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            builder.withCachingEnabled();
        }
        return builder;
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