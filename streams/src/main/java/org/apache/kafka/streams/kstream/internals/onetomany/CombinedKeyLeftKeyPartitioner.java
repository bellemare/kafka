package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class CombinedKeyLeftKeyPartitioner<KL,KR,V> implements StreamPartitioner<CombinedKey<KL,KR>, V> {

	private final Serializer<KL> keySerializer;
	private final String topic;
	private StreamPartitioner streamPartitioner;

	//Use the default defaultPartitioner
	public CombinedKeyLeftKeyPartitioner(CombinedKeySerde<KL,KR> keySerde, String topic){
		this.keySerializer = keySerde.getLeftSerializer();
		this.topic = topic;
	}

	//Use a custom defaultPartitioner.
	public CombinedKeyLeftKeyPartitioner(CombinedKeySerde<KL,KR> keySerde, String topic, StreamPartitioner<KL,?> foo){
		this.keySerializer = keySerde.getLeftSerializer();
		this.topic = topic;
		this.streamPartitioner = foo;
	}

	@Override
	public Integer partition(CombinedKey<KL, KR> key, V value, int numPartitions) {
		if (null == streamPartitioner) {
			byte[] keyBytes = keySerializer.serialize(topic, key.getForeignKey());
			//TODO - Evaluate breaking this out of the DefaultPartitioner Producer into an accessible function.
			return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
		} else {
			return streamPartitioner.partition(key.getForeignKey(), value, numPartitions);
		}
	}
}