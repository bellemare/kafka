package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionResponseWrapperDeserializer<V> implements Deserializer<SubscriptionResponseWrapper<V>> {
    final private Deserializer<V> deserializer;

    public SubscriptionResponseWrapperDeserializer(Deserializer<V> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Do nothing
    }

    @Override
    public SubscriptionResponseWrapper<V> deserialize(String topic, byte[] data) {
        //{16-bytes Hash}{n-bytes serialized data}
        final ByteBuffer buf = ByteBuffer.wrap(data);
        final long[] hash = new long[2];
        hash[0] = buf.getLong();
        hash[1] = buf.getLong();
        final byte[] serializedValue = (data.length == 16 ? null : new byte[data.length - 16]);
        if (serializedValue != null)
            buf.get(serializedValue, 0, data.length-16);
        V value = deserializer.deserialize(topic, serializedValue);
        return new SubscriptionResponseWrapper<>(hash, value);
    }

    @Override
    public void close() {
        //Do nothing
    }
}
