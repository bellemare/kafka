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

public class CombinedKeyByPrimaryKeyPartitioner<KO, K, V> implements StreamPartitioner<CombinedKey<KO, K>, V> {

    private final Serializer<K> keySerializer;
    private final String topic;
    private StreamPartitioner streamPartitioner;

    //Use the default partitioner
    public CombinedKeyByPrimaryKeyPartitioner(final CombinedKeySerde<KO, K> keySerde, final String topic) {
        this(keySerde, topic, null);
    }

    //Use a custom partitioner.
    public CombinedKeyByPrimaryKeyPartitioner(final CombinedKeySerde<KO, K> keySerde, final String topic, final StreamPartitioner<KO, ?> streamPartitioner) {
        this.keySerializer = keySerde.getPrimaryKeySerializer();
        this.topic = topic;
        this.streamPartitioner = streamPartitioner;
    }

    @Override
    public Integer partition(final String topic, final CombinedKey<KO, K> key, final V value, final int numPartitions) {
        if (null == streamPartitioner) {
            final byte[] keyBytes = keySerializer.serialize(topic, key.getPrimaryKey());
            //TODO - Evaluate breaking this out of the DefaultPartitioner Producer into an accessible function.
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        } else {
            return streamPartitioner.partition(topic, key.getPrimaryKey(), value, numPartitions);
        }
    }
}