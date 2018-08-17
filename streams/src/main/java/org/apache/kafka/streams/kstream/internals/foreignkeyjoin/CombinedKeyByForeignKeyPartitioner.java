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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class CombinedKeyByForeignKeyPartitioner<KL, KR, V> implements StreamPartitioner<CombinedKey<KL, KR>, V> {

    private final Serializer<KL> keySerializer;
    private final String topic;
    private StreamPartitioner streamPartitioner;

    //Use the default partitioner
    public CombinedKeyByForeignKeyPartitioner(final CombinedKeySerde<KL, KR> keySerde, final String topic) {
        this.keySerializer = keySerde.getForeignKeySerializer();
        this.topic = topic;
    }

    //Use a custom partitioner.
    public CombinedKeyByForeignKeyPartitioner(final CombinedKeySerde<KL, KR> keySerde, final String topic, final StreamPartitioner<KL, ?> foo) {
        this.keySerializer = keySerde.getForeignKeySerializer();
        this.topic = topic;
        this.streamPartitioner = foo;
    }

    @Override
    public Integer partition(final String topic, final CombinedKey<KL, KR> key, final V value, final int numPartitions) {
        if (null == streamPartitioner) {
            final byte[] keyBytes = keySerializer.serialize(topic, key.getForeignKey());
            //TODO - Evaluate breaking this out of the DefaultPartitioner Producer into an accessible function.
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        } else {
            return streamPartitioner.partition(topic, key.getForeignKey(), value, numPartitions);
        }
    }
}