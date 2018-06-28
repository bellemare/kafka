package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class PartialKeyPartitioner<K,V,K1> implements StreamPartitioner<K, V> {

	private ValueMapper<K, K1> extractor;
	private Serializer<K1> keySerializer;

	public PartialKeyPartitioner(ValueMapper<K, K1> extractor, Serde<K1> keySerde){
		this.keySerializer = keySerde.serializer();
		this.extractor = extractor;
	}

	@Override
	public Integer partition(String topic, K key, V value, int numPartitions) {
		/**
		 * maybe allow user to supply inner Streampartitioner?
		 * only works if left side is murmurpartitioned in this case
		 */
		return Utils.toPositive(Utils.murmur2(keySerializer.serialize(topic, extractor.apply(key)))) % numPartitions;
	}
}