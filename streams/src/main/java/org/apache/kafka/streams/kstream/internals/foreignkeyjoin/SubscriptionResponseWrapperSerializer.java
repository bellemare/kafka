package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionResponseWrapperSerializer<V> implements Serializer<SubscriptionResponseWrapper<V>> {
    private final Serializer<V> serializer;

    public SubscriptionResponseWrapperSerializer(Serializer<V> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        //Do nothing
    }

    @Override
    public byte[] serialize(String topic, SubscriptionResponseWrapper<V> data) {
        //{16-bytes Hash}{n-bytes serialized data}
        byte[] serializedData = serializer.serialize(topic, data.getForeignValue());
        int length = (serializedData == null ? 0 : serializedData.length);
        final ByteBuffer buf = ByteBuffer.allocate(16 + length);
        long[] elem = data.getOriginalValueHash();
        buf.putLong(elem[0]);
        buf.putLong(elem[1]);
        if (serializedData != null)
            buf.put(serializedData);
        return buf.array();
    }

    @Override
    public void close() {
        //Do nothing
    }
}
