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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKey;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeyByForeignKeyPartitioner;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.CombinedKeySerde;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.ForeignKeySingleLookupProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.HighwaterResolverProcessorSupplier;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.KTableKTablePrefixScanJoin;
import org.apache.kafka.streams.kstream.internals.foreignkeyjoin.KTableRepartitionerProcessorSupplier;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * The implementation class of {@link KTable}.
 *
 * @param <K> the key type
 * @param <S> the source's (parent's) value type
 * @param <V> the value type
 */
public class KTableImpl<K, S, V> extends AbstractStream<K> implements KTable<K, V> {

    static final String SOURCE_NAME = "KTABLE-SOURCE-";

    static final String STATE_STORE_NAME = "STATE-STORE-";

    private static final String FILTER_NAME = "KTABLE-FILTER-";

    private static final String JOINTHIS_NAME = "KTABLE-JOINTHIS-";

    private static final String JOINOTHER_NAME = "KTABLE-JOINOTHER-";

    private static final String MAPVALUES_NAME = "KTABLE-MAPVALUES-";

    private static final String MERGE_NAME = "KTABLE-MERGE-";

    private static final String SELECT_NAME = "KTABLE-SELECT-";

    private static final String TOSTREAM_NAME = "KTABLE-TOSTREAM-";

    private static final String TRANSFORMVALUES_NAME = "KTABLE-TRANSFORMVALUES-";

    public static final String BY_PREFIX = "KTABLE-JOIN-BYPREFIX-";

    private static final String REPARTITION_NAME = "KTABLE-REPARTITION-";

    private final ProcessorSupplier<?, ?> processorSupplier;

    private final String queryableStoreName;
    private final boolean isQueryable;

    private boolean sendOldValues = false;
    private final Serde<K> keySerde;
    private final Serde<V> valSerde;

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = null;
        this.valSerde = null;
        this.isQueryable = isQueryable;
    }

    public KTableImpl(final InternalStreamsBuilder builder,
                      final String name,
                      final ProcessorSupplier<?, ?> processorSupplier,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde,
                      final Set<String> sourceNodes,
                      final String queryableStoreName,
                      final boolean isQueryable) {
        super(builder, name, sourceNodes);
        this.processorSupplier = processorSupplier;
        this.queryableStoreName = queryableStoreName;
        this.keySerde = keySerde;
        this.valSerde = valSerde;
        this.isQueryable = isQueryable;
    }

    @Override
    public String queryableStoreName() {
        if (!isQueryable) {
            return null;
        } else {
            return this.queryableStoreName;
        }
    }

    private KTable<K, V> doFilter(final Predicate<? super K, ? super V> predicate,
                                  final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materialized,
                                  final boolean filterNot) {
        final String name = builder.newProcessorName(FILTER_NAME);

        // only materialize if the state store is queryable
        final boolean shouldMaterialize = materialized != null && materialized.isQueryable();

        KTableProcessorSupplier<K, V, V> processorSupplier = new KTableFilter<>(this,
                predicate,
                filterNot,
                shouldMaterialize ? materialized.storeName() : null);

        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);

        if (shouldMaterialize) {
            this.builder.internalTopologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(materialized).materialize(), name);
        }

        return new KTableImpl<>(builder,
                name,
                processorSupplier,
                this.keySerde,
                this.valSerde,
                sourceNodes,
                shouldMaterialize ? materialized.storeName() : this.queryableStoreName,
                shouldMaterialize);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, null, false);
    }

    @Override
    public KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                               final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, FILTER_NAME);

        return doFilter(predicate, materializedInternal, false);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        return doFilter(predicate, null, true);
    }

    @Override
    public KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                                  final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(predicate, "predicate can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, V, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, FILTER_NAME);

        return doFilter(predicate, materializedInternal, true);
    }

    private <VR> KTable<K, VR> doMapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                           final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final String name = builder.newProcessorName(MAPVALUES_NAME);

        // only materialize if the state store is queryable
        final boolean shouldMaterialize = materialized != null && materialized.isQueryable();

        final KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableMapValues<>(
                this,
                mapper,
                shouldMaterialize ? materialized.storeName() : null);

        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);

        if (shouldMaterialize) {
            this.builder.internalTopologyBuilder.addStateStore(new KeyValueStoreMaterializer<>(materialized).materialize(), name);
        }

        return new KTableImpl<>(builder, name, processorSupplier, sourceNodes, shouldMaterialize ? materialized.storeName() : this.queryableStoreName, shouldMaterialize);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(withKey(mapper), null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        return doMapValues(mapper, null);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, MAPVALUES_NAME);

        return doMapValues(withKey(mapper), materializedInternal);
    }

    @Override
    public <VR> KTable<K, VR> mapValues(final ValueMapperWithKey<? super K, ? super V, ? extends VR> mapper,
                                        final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(mapper, "mapper can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, MAPVALUES_NAME);

        return doMapValues(mapper, materializedInternal);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final String... stateStoreNames) {
        return doTransformValues(transformerSupplier, null, stateStoreNames);
    }

    @Override
    public <VR> KTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                              final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                              final String... stateStoreNames) {
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, TRANSFORMVALUES_NAME);

        return doTransformValues(transformerSupplier, materializedInternal, stateStoreNames);
    }

    private <VR> KTable<K, VR> doTransformValues(final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> transformerSupplier,
                                                 final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                                 final String... stateStoreNames) {
        Objects.requireNonNull(stateStoreNames, "stateStoreNames");

        final String name = builder.newProcessorName(TRANSFORMVALUES_NAME);

        final boolean shouldMaterialize = materialized != null && materialized.isQueryable();

        final KTableProcessorSupplier<K, V, VR> processorSupplier = new KTableTransformValues<>(
            this,
            transformerSupplier,
            shouldMaterialize ? materialized.storeName() : null);

        builder.internalTopologyBuilder.addProcessor(name, processorSupplier, this.name);

        if (stateStoreNames.length > 0) {
            builder.internalTopologyBuilder.connectProcessorAndStateStores(name, stateStoreNames);
        }

        if (shouldMaterialize) {
            builder.internalTopologyBuilder.addStateStore(
                new KeyValueStoreMaterializer<>(materialized).materialize(),
                name);
        }

        return new KTableImpl<>(
            builder,
            name,
            processorSupplier,
            sourceNodes,
            shouldMaterialize ? materialized.storeName() : this.queryableStoreName,
            shouldMaterialize);
    }

    @Override
    public KStream<K, V> toStream() {
        String name = builder.newProcessorName(TOSTREAM_NAME);

        builder.internalTopologyBuilder.addProcessor(name, new KStreamMapValues<>(new ValueMapperWithKey<K, Change<V>, V>() {
            @Override
            public V apply(final K key, final Change<V> change) {
                return change.newValue;
            }
        }), this.name);

        return new KStreamImpl<>(builder, name, sourceNodes, false);
    }

    @Override
    public <K1> KStream<K1, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends K1> mapper) {
        return toStream().selectKey(mapper);
    }

    @Override
    public <V1, R> KTable<K, R> join(final KTable<K, V1> other,
                                     final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, false, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                       final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, MERGE_NAME);

        return doJoin(other, joiner, materializedInternal, false, false);
    }

    @Override
    public <V1, R> KTable<K, R> outerJoin(final KTable<K, V1> other,
                                          final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, true);
    }

    @Override
    public <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                            final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                            final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, MERGE_NAME);

        return doJoin(other, joiner, materializedInternal, true, true);
    }

    @Override
    public <V1, R> KTable<K, R> leftJoin(final KTable<K, V1> other,
                                         final ValueJoiner<? super V, ? super V1, ? extends R> joiner) {
        return doJoin(other, joiner, null, true, false);
    }

    @Override
    public <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                           final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                           final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized) {
        final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materializedInternal = new MaterializedInternal<>(materialized);
        materializedInternal.generateStoreNameIfNeeded(builder, MERGE_NAME);
        return doJoin(other, joiner, materializedInternal, true, false);
    }

    @SuppressWarnings("unchecked")
    private <VO, VR> KTable<K, VR> doJoin(final KTable<K, VO> other,
                                          final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                          final boolean leftOuter,
                                          final boolean rightOuter) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        final String internalQueryableName = materialized == null ? null : materialized.storeName();
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);
        final KTable<K, VR> result = buildJoin((AbstractStream<K>) other,
                                               joiner,
                                               leftOuter,
                                               rightOuter,
                                               joinMergeName,
                                               internalQueryableName);

        // only materialize if specified in Materialized
        if (materialized != null) {
            final StoreBuilder<KeyValueStore<K, VR>> storeBuilder
                    = new KeyValueStoreMaterializer<>(materialized).materialize();
            builder.internalTopologyBuilder.addStateStore(storeBuilder, joinMergeName);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <V1, R> KTable<K, R> buildJoin(final AbstractStream<K> other,
                                           final ValueJoiner<? super V, ? super V1, ? extends R> joiner,
                                           final boolean leftOuter,
                                           final boolean rightOuter,
                                           final String joinMergeName,
                                           final String internalQueryableName) {
        final Set<String> allSourceNodes = ensureJoinableWith(other);

        if (leftOuter) {
            enableSendingOldValues();
        }
        if (rightOuter) {
            ((KTableImpl) other).enableSendingOldValues();
        }

        final String joinThisName = builder.newProcessorName(JOINTHIS_NAME);
        final String joinOtherName = builder.newProcessorName(JOINOTHER_NAME);


        final KTableKTableAbstractJoin<K, R, V, V1> joinThis;
        final KTableKTableAbstractJoin<K, R, V1, V> joinOther;

        if (!leftOuter) { // inner
            joinThis = new KTableKTableInnerJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableInnerJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else if (!rightOuter) { // left
            joinThis = new KTableKTableLeftJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableRightJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        } else { // outer
            joinThis = new KTableKTableOuterJoin<>(this, (KTableImpl<K, ?, V1>) other, joiner);
            joinOther = new KTableKTableOuterJoin<>((KTableImpl<K, ?, V1>) other, this, reverseJoiner(joiner));
        }

        final KTableKTableJoinMerger<K, R> joinMerge = new KTableKTableJoinMerger<>(
                new KTableImpl<K, V, R>(builder, joinThisName, joinThis, sourceNodes, this.queryableStoreName, false),
                new KTableImpl<K, V1, R>(builder, joinOtherName, joinOther, ((KTableImpl<K, ?, ?>) other).sourceNodes,
                        ((KTableImpl<K, ?, ?>) other).queryableStoreName, false),
                internalQueryableName
        );

        builder.internalTopologyBuilder.addProcessor(joinThisName, joinThis, this.name);
        builder.internalTopologyBuilder.addProcessor(joinOtherName, joinOther, ((KTableImpl) other).name);
        builder.internalTopologyBuilder.addProcessor(joinMergeName, joinMerge, joinThisName, joinOtherName);
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinThisName, ((KTableImpl) other).valueGetterSupplier().storeNames());
        builder.internalTopologyBuilder.connectProcessorAndStateStores(joinOtherName, valueGetterSupplier().storeNames());
        return new KTableImpl<>(builder, joinMergeName, joinMerge, allSourceNodes, internalQueryableName, internalQueryableName != null);
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector) {
        return this.groupBy(selector, Serialized.<K1, V1>with(null, null));
    }

    @Override
    public <K1, V1> KGroupedTable<K1, V1> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<K1, V1>> selector,
                                                  final Serialized<K1, V1> serialized) {
        Objects.requireNonNull(selector, "selector can't be null");
        Objects.requireNonNull(serialized, "serialized can't be null");
        String selectName = builder.newProcessorName(SELECT_NAME);

        KTableProcessorSupplier<K, V, KeyValue<K1, V1>> selectSupplier = new KTableRepartitionMap<>(this, selector);

        // select the aggregate key and values (old and new), it would require parent to send old values
        builder.internalTopologyBuilder.addProcessor(selectName, selectSupplier, this.name);
        this.enableSendingOldValues();
        final SerializedInternal<K1, V1> serializedInternal  = new SerializedInternal<>(serialized);
        return new KGroupedTableImpl<>(builder,
                                       selectName,
                                       this.name,
                                       serializedInternal.keySerde(),
                                       serializedInternal.valueSerde());
    }

    @SuppressWarnings("unchecked")
    KTableValueGetterSupplier<K, V> valueGetterSupplier() {
        if (processorSupplier instanceof KTableSource) {
            KTableSource<K, V> source = (KTableSource<K, V>) processorSupplier;
            return new KTableSourceValueGetterSupplier<>(source.storeName);
        } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
            return ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).view();
        } else {
            return ((KTableProcessorSupplier<K, S, V>) processorSupplier).view();
        }
    }

    @SuppressWarnings("unchecked")
    void enableSendingOldValues() {
        if (!sendOldValues) {
            if (processorSupplier instanceof KTableSource) {
                KTableSource<K, ?> source = (KTableSource<K, V>) processorSupplier;
                source.enableSendingOldValues();
            } else if (processorSupplier instanceof KStreamAggProcessorSupplier) {
                ((KStreamAggProcessorSupplier<?, K, S, V>) processorSupplier).enableSendingOldValues();
            } else {
                ((KTableProcessorSupplier<K, S, V>) processorSupplier).enableSendingOldValues();
            }
            sendOldValues = true;
        }
    }

    boolean sendingOldValueEnabled() {
        return sendOldValues;
    }

    public <VR, KO, VO> KTable<K, VR> joinOnForeignKey(final KTable<KO, VO> other,
                                                       final ValueMapper<V, KO> keyExtractor,
                                                       final ValueJoiner<V, VO, VR> joiner,
                                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                                       final StreamPartitioner<KO, ?> foreignKeyPartitioner,
                                                       final Serialized<K, V> thisSerialized,
                                                       final Serialized<KO, VO> otherSerialized,
                                                       final Serialized<K, VR> joinedSerialized) {

        return doJoinOnForeignKey(other, keyExtractor, joiner, new MaterializedInternal<>(materialized),
                foreignKeyPartitioner, thisSerialized, otherSerialized, joinedSerialized);
    }

    public <VR, KO, VO> KTable<K, VR> joinOnForeignKey(final KTable<KO, VO> other,
                                                       final ValueMapper<V, KO> keyExtractor,
                                                       final ValueJoiner<V, VO, VR> joiner,
                                                       final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                                       final Serialized<K, V> thisSerialized,
                                                       final Serialized<KO, VO> otherSerialized,
                                                       final Serialized<K, VR> joinedSerialized) {

        return doJoinOnForeignKey(other, keyExtractor, joiner, new MaterializedInternal<>(materialized),
                null, thisSerialized, otherSerialized, joinedSerialized);
    }

    @SuppressWarnings("unchecked")
    private <VR, KO, VO> KTable<K, VR> doJoinOnForeignKey(final KTable<KO, VO> other,
                                                          final ValueMapper<V, KO> keyExtractor,
                                                          final ValueJoiner<V, VO, VR> joiner,
                                                          final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                                          final StreamPartitioner<KO, ?> foreignKeyPartitioner,
                                                          final Serialized<K, V> thisSerialized,
                                                          final Serialized<KO, VO> otherSerialized,
                                                          final Serialized<K, VR> joinedSerialized) {
        Objects.requireNonNull(other, "other can't be null");
        Objects.requireNonNull(keyExtractor, "keyExtractor can't be null");
        Objects.requireNonNull(joiner, "joiner can't be null");
        Objects.requireNonNull(materialized, "materialized can't be null");

        final KTable<K, VR> result = buildJoinOnForeignKey(other,
                keyExtractor,
                joiner,
                materialized,
                foreignKeyPartitioner,
                new SerializedInternal<>(thisSerialized),
                new SerializedInternal<>(otherSerialized),
                new SerializedInternal<>(joinedSerialized));

        return result;
    }

    private <VR, KO, VO> KTable<K, VR> buildJoinOnForeignKey(final KTable<KO, VO> other,
                                                             final ValueMapper<V, KO> keyExtractor,
                                                             final ValueJoiner<V, VO, VR> joiner,
                                                             final MaterializedInternal<K, VR, KeyValueStore<Bytes, byte[]>> materialized,
                                                             final StreamPartitioner<KO, ?> foreignKeyPartitioner,
                                                             final SerializedInternal<K, V> thisSerialized,
                                                             final SerializedInternal<KO, VO> otherSerialized,
                                                             final SerializedInternal<K, VR> joinedSerialized) {

        ((KTableImpl<?, ?, ?>) other).enableSendingOldValues();
        enableSendingOldValues();

        final String repartitionerName = builder.newProcessorName(REPARTITION_NAME);
        final String repartitionTopicName = JOINOTHER_NAME + repartitionerName + "-TOPIC";
        final String repartitionProcessorName = repartitionerName + "-" + SELECT_NAME;
        final String repartitionSourceName = repartitionerName + "-SOURCE";
        final String repartitionSinkName = repartitionerName + "-SINK";
        final String joinOneToOneName = repartitionerName + "-TABLE";

        // repartition original => intermediate topic
        final KTableRepartitionerProcessorSupplier<K, KO, V> repartitionProcessor =
                new KTableRepartitionerProcessorSupplier<>(keyExtractor);

        final CombinedKeySerde<KO, K> combinedKeySerde = new CombinedKeySerde<>(otherSerialized.keySerde(), thisSerialized.keySerde());

        //Create the partitioner that will partition CombinedKey on just the foreign portion (right) of the combinedKey.
        final CombinedKeyByForeignKeyPartitioner<KO, K, V> partitioner;
        if (null == foreignKeyPartitioner)
            partitioner = new CombinedKeyByForeignKeyPartitioner<>(combinedKeySerde, repartitionTopicName);
        else
            partitioner = new CombinedKeyByForeignKeyPartitioner<>(combinedKeySerde, repartitionTopicName, foreignKeyPartitioner);

        //The processor for this table. It does two main things:
        // 1) Loads the data into a stateStore, to be accessed by the KTableKTablePrefixJoin processor.
        // 2) Drives the join logic from this.
        //    Returns the data keyed on K. Discards the CombinedKey as it is no longer needed after this stage.
        final String joinByPrefixName = builder.newProcessorName(BY_PREFIX);

        final String thisStateStoreName = builder.newStoreName(STATE_STORE_NAME);
        final ForeignKeySingleLookupProcessorSupplier<K, KO, V, VO, V> joinOneToOne =
                new ForeignKeySingleLookupProcessorSupplier(thisStateStoreName, ((KTableImpl<?, ?, ?>) other).valueGetterSupplier(), joiner);

        final KeyValueBytesStoreSupplier thisRocksDBRef = new RocksDbKeyValueBytesStoreSupplier(thisStateStoreName);
        final StateStore prefixScannableDBRef = thisRocksDBRef.get();
        final Materialized foreignMaterialized = Materialized.<CombinedKey<KO, K>, V, KeyValueStore<Bytes, byte[]>>as(prefixScannableDBRef.name())
                //Need all values to be immediately available in the rocksDB store.
                //No easy way to flush cache prior to prefixScan, so caching is disabled on this store.
                .withCachingDisabled()
                .withKeySerde(combinedKeySerde)
                .withValueSerde(thisSerialized.valueSerde());
        final MaterializedInternal<CombinedKey<KO, K>, V, KeyValueStore<Bytes, byte[]>> repartitionedPrefixScannableStore =
                new MaterializedInternal<CombinedKey<KO, K>, V, KeyValueStore<Bytes, byte[]>>(foreignMaterialized);

        //Performs foreign-key-driven updates (ie: new One, updates the Many).
        final KTablePrefixValueGetterSupplier<CombinedKey<KO, K>, V> oneToOneProcessor = joinOneToOne.valueGetterSupplier();
        final ProcessorSupplier<KO, Change<VO>> joinByPrefix = new KTableKTablePrefixScanJoin<>(oneToOneProcessor, joiner, prefixScannableDBRef);

        //Need to write all updates to a given K back to the same partition, as at this point in the topology
        //everything is partitioned on KO from the previous repartition step.
        final String finalRepartitionerName = builder.newProcessorName(REPARTITION_NAME);
        final String finalRepartitionTopicName = JOINOTHER_NAME + finalRepartitionerName + "-TOPIC";

        final String finalRepartitionSourceName = finalRepartitionerName + "-SOURCE";
        final String finalRepartitionSinkName = finalRepartitionerName + "-SINK";
        final String finalRepartitionTableName = finalRepartitionerName + "-TABLE";

        //Create the processor to resolve the propagation wrappers against the highwater mark for a given K.
        final HighwaterResolverProcessorSupplier<K, VR> highwaterProcessor = new HighwaterResolverProcessorSupplier<>(finalRepartitionTableName);
        final String highwaterProcessorName = builder.newProcessorName(KTableImpl.SOURCE_NAME);

        final KeyValueBytesStoreSupplier highwaterRdbs = new RocksDbKeyValueBytesStoreSupplier(finalRepartitionTableName);
        final Materialized highwaterMat = Materialized.<K, Long, KeyValueStore<Bytes, byte[]>>as(highwaterRdbs.get().name())
                .withKeySerde(thisSerialized.keySerde())
                .withValueSerde(Serdes.Long());
        final MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>> highwaterMatInternal =
                new MaterializedInternal<K, Long, KeyValueStore<Bytes, byte[]>>(highwaterMat);

        final KTableSource<K, VR> outputProcessor = new KTableSource<>(materialized.storeName());
        final String outputProcessorName = builder.newProcessorName(SOURCE_NAME);

        final HashSet<String> copartitions = new HashSet<>();
        copartitions.add(repartitionSourceName);
        copartitions.add(finalRepartitionSourceName);
        copartitions.addAll(((KTableImpl<?, ?, ?>) other).sourceNodes);
        copartitions.addAll(sourceNodes);
        builder.internalTopologyBuilder.copartitionSources(copartitions);

        final ProcessorParameters repartitionProcessorParameters = new ProcessorParameters<>(
                repartitionProcessor,
                repartitionProcessorName
        );

        final ProcessorParameters joinOneToOneProcessorParameters = new ProcessorParameters<>(
                joinOneToOne,
                joinOneToOneName
        );

        final ProcessorParameters<KO, Change<VO>> joinByPrefixProcessorParameters = new ProcessorParameters<>(
                joinByPrefix,
                joinByPrefixName
        );

        final ProcessorParameters<K, VR> highwaterProcessorParameters = new ProcessorParameters<>(
                highwaterProcessor,
                highwaterProcessorName
        );

        final ProcessorParameters<K, VR> outputProcessorParameters = new ProcessorParameters<>(
                outputProcessor,
                outputProcessorName
        );



        internalTopologyBuilder().addInternalTopic(repartitionTopicName);
        internalTopologyBuilder().addProcessor(repartitionProcessorParameters.processorName(),
                repartitionProcessorParameters.processorSupplier(),
                this.name);
        //Repartition to the KR prefix of the CombinedKey.
        internalTopologyBuilder().addSink(repartitionSinkName, repartitionTopicName,
                combinedKeySerde.serializer(), thisSerialized.valueSerde().serializer(),
                partitioner, repartitionProcessorParameters.processorName());

        internalTopologyBuilder().addSource(null, repartitionSourceName, new FailOnInvalidTimestamp(),
                combinedKeySerde.deserializer(), thisSerialized.valueSerde().deserializer(), repartitionTopicName);

        internalTopologyBuilder().addProcessor(joinOneToOneProcessorParameters.processorName(),
                joinOneToOneProcessorParameters.processorSupplier(),
                repartitionSourceName);

        //Connect the left processor with the state store.
        internalTopologyBuilder().addStateStore(new KeyValueStoreMaterializer<>(repartitionedPrefixScannableStore).materialize(), joinOneToOneProcessorParameters.processorName());
        //Add the right processor to the topology.
        internalTopologyBuilder().addProcessor(joinByPrefixProcessorParameters.processorName(), joinByPrefixProcessorParameters.processorSupplier(), ((KTableImpl<?, ?, ?>) other).name);

        internalTopologyBuilder().addInternalTopic(finalRepartitionTopicName);
        //Repartition back to the original partitioning structure
        internalTopologyBuilder().addSink(finalRepartitionSinkName, finalRepartitionTopicName,
                thisSerialized.keySerde().serializer(), joinedSerialized.valueSerde().serializer(),
                null,
                joinByPrefixProcessorParameters.processorName(), joinOneToOneProcessorParameters.processorName());
        internalTopologyBuilder().addSource(null, finalRepartitionSourceName, new FailOnInvalidTimestamp(),
                thisSerialized.keySerde().deserializer(), joinedSerialized.valueSerde().deserializer(), finalRepartitionTopicName);

        //Connect highwaterProcessor to source, add the state store, and connect the statestore with the processor.
        internalTopologyBuilder().addProcessor(highwaterProcessorParameters.processorName(), highwaterProcessorParameters.processorSupplier(), finalRepartitionSourceName);
        internalTopologyBuilder().addStateStore(new KeyValueStoreMaterializer<>(highwaterMatInternal).materialize(), highwaterProcessorParameters.processorName());
        internalTopologyBuilder().connectProcessorAndStateStores(highwaterProcessorParameters.processorName(), finalRepartitionTableName);
        //Connect the first-stage processors to the source state stores.
        internalTopologyBuilder().connectProcessorAndStateStores(joinByPrefixProcessorParameters.processorName(), prefixScannableDBRef.name());
        internalTopologyBuilder().connectProcessorAndStateStores(joinOneToOneProcessorParameters.processorName(), ((KTableImpl<?, ?, ?>) other).queryableStoreName);

        //Hook up the highwatermark output to KTableSource Processor
        internalTopologyBuilder().addProcessor(outputProcessorParameters.processorName(), outputProcessorParameters.processorSupplier(), highwaterProcessorParameters.processorName());

        final StoreBuilder<KeyValueStore<K, VR>> storeBuilder
                = new KeyValueStoreMaterializer<>(materialized).materialize();
        internalTopologyBuilder().addStateStore(storeBuilder, outputProcessorParameters.processorName());
        internalTopologyBuilder().connectProcessorAndStateStores(outputProcessorParameters.processorName(), storeBuilder.name());

        return new KTableImpl<>(builder, outputProcessorName, outputProcessor, thisSerialized.keySerde(),
                joinedSerialized.valueSerde(), Collections.singleton(finalRepartitionSourceName),
                materialized.storeName(), true);
    }
}
