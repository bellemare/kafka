package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class SimpleKeyPartitioner<K,V> implements StreamPartitioner<K, V> {
    private Serializer<K> keySerializer;
    private String topic;

    public SimpleKeyPartitioner(Serde<K> keySerde, String topic){
        this.keySerializer = keySerde.serializer();
        this.topic = topic;
    }

    @Override
    public Integer partition(K key, V value, int numPartitions) {
        /**
         * maybe allow user to supply inner Streampartitioner?
         * only works if left side is murmurpartitioned in this case
         */
        return Utils.toPositive(Utils.murmur2(keySerializer.serialize(topic, key))) % numPartitions;
    }
}