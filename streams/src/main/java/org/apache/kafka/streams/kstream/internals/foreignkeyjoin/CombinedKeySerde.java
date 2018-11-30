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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Factory for creating CombinedKey serializers / deserializers.
 */
public class CombinedKeySerde<KO, K> implements Serde<CombinedKey<KO, K>> {
    final private Serializer<K> primaryKeySerializer;
    final private Deserializer<K> primaryKeyDeserializer;
    final private Serializer<KO> foreignKeySerializer;
    final private Deserializer<KO> foreignKeyDeserializer;
    final private Serializer<CombinedKey<KO, K>> serializer;
    final private Deserializer<CombinedKey<KO, K>> deserializer;

    public CombinedKeySerde(final Serde<KO> foreignKeySerde, final Serde<K> primaryKeySerde) {
        this.primaryKeySerializer = primaryKeySerde.serializer();
        this.primaryKeyDeserializer = primaryKeySerde.deserializer();
        this.foreignKeyDeserializer = foreignKeySerde.deserializer();
        this.foreignKeySerializer = foreignKeySerde.serializer();
        this.serializer = new CombinedKeySerializer<>(foreignKeySerializer, primaryKeySerializer);
        this.deserializer = new CombinedKeyDeserializer<>(foreignKeyDeserializer, primaryKeyDeserializer);
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        primaryKeySerializer.configure(configs, isKey);
        foreignKeySerializer.configure(configs, isKey);
        primaryKeyDeserializer.configure(configs, isKey);
        foreignKeyDeserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        primaryKeyDeserializer.close();
        foreignKeyDeserializer.close();
        primaryKeySerializer.close();
        foreignKeySerializer.close();
    }

    @Override
    public Serializer<CombinedKey<KO, K>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<CombinedKey<KO, K>> deserializer() {
        return deserializer;
    }

    public Serializer<KO> getForeignKeySerializer() {
        return this.foreignKeySerializer;
    }

    public Serializer<K> getPrimaryKeySerializer() {
        return this.primaryKeySerializer;
    }
}
