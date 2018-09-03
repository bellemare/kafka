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

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

class CombinedKeySerializer<KF, KP> implements Serializer<CombinedKey<KF, KP>> {

    private final Serializer<KF> foreignKeySerializer;
    private final Serializer<KP> primaryKeySerializer;

    public CombinedKeySerializer(final Serializer<KF> foreignKeySerializer, final Serializer<KP> primaryKeySerializer) {
        this.foreignKeySerializer = foreignKeySerializer;
        this.primaryKeySerializer = primaryKeySerializer;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        //Don't need to configure, they are already configured. This is just a wrapper.
    }

    @Override
    public byte[] serialize(final String topic, final CombinedKey<KF, KP> data) {
        //{4-byte foreignKeyLength}{foreignKeySerialized}{primaryKeySerialized}
        //? bytes
        final byte[] foreignKeySerializedData = foreignKeySerializer.serialize(topic, data.getForeignKey());
        //4 bytes
        final byte[] foreignKeyByteSize = numToBytes(foreignKeySerializedData.length);

        if (data.getPrimaryKey() != null) {
            //? bytes
            final byte[] primaryKeySerializedData = primaryKeySerializer.serialize(topic, data.getPrimaryKey());

            ByteBuffer buf = ByteBuffer.allocate(4 + foreignKeySerializedData.length + primaryKeySerializedData.length);
            buf.put(foreignKeyByteSize);
            buf.put(foreignKeySerializedData);
            buf.put(primaryKeySerializedData);
            return buf.array();
        } else {
            ByteBuffer buf = ByteBuffer.allocate(4 + foreignKeySerializedData.length);
            buf.put(foreignKeyByteSize);
            buf.put(foreignKeySerializedData);
            return buf.array();
        }
    }

    private byte[] numToBytes(final int num) {
        final ByteBuffer wrapped = ByteBuffer.allocate(4);
        wrapped.putInt(num);
        return wrapped.array();
    }

    @Override
    public void close() {
        this.foreignKeySerializer.close();
        this.primaryKeySerializer.close();
    }
}
