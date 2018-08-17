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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

class CombinedKeyDeserializer<KF, KP> implements Deserializer<CombinedKey<KF, KP>> {

    private final Deserializer<KF> foreignKeyDeserializer;
    private final Deserializer<KP> primaryKeyDeserializer;


    public CombinedKeyDeserializer(final Deserializer<KF> foreignKeyDeserializer, final Deserializer<KP> primaryKeyDeserializer) {
        this.foreignKeyDeserializer = foreignKeyDeserializer;
        this.primaryKeyDeserializer = primaryKeyDeserializer;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        //Don't need to configure them, as they are already configured. This is only a wrapper.
    }

    @Override
    public CombinedKey<KF, KP> deserialize(final String topic, final byte[] data) {
        //{4-byte foreignKeyLength}{foreignKeySerialized}{4-bytePrimaryKeyLength}{primaryKeySerialized}

        final byte[] fkCount = Arrays.copyOfRange(data, 0, 4);
        final int foreignKeyLength = fourBytesToInt(fkCount);
        final byte[] foreignKeyRaw = Arrays.copyOfRange(data, 4, 4 + foreignKeyLength);

        final KF foreignKey = foreignKeyDeserializer.deserialize(topic, foreignKeyRaw);

        if (data.length == 4 + foreignKeyLength) {
            return new CombinedKey<>(foreignKey);
        } else {

            final byte[] primaryCount = Arrays.copyOfRange(data, 4 + foreignKeyLength, 4 + foreignKeyLength + 4);
            final int primaryKeyLength = fourBytesToInt(primaryCount);

            final byte[] primaryKeyRaw = Arrays.copyOfRange(data, 4 + foreignKeyLength + 4, 4 + foreignKeyLength + 4 + primaryKeyLength);
            final KP primaryKey = primaryKeyDeserializer.deserialize(topic, primaryKeyRaw);
            return new CombinedKey<>(foreignKey, primaryKey);
        }
    }

    @Override
    public void close() {
        foreignKeyDeserializer.close();
        primaryKeyDeserializer.close();
    }

    private int fourBytesToInt(final byte[] fourBytes) {
        if (fourBytes.length != 4) {
            throw new ArrayIndexOutOfBoundsException("Expected 4 bytes when deserializing the CombinedKey! Found " + fourBytes.length);
        }
        final ByteBuffer wrapped = ByteBuffer.wrap(fourBytes);
        return wrapped.getInt();
    }

}
