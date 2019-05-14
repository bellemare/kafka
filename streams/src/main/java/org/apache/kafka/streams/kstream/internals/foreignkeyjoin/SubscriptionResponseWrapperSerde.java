package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionResponseWrapperSerde<V> implements Serde<SubscriptionResponseWrapper<V>> {
    private final SubscriptionResponseWrapperSerializer<V> serializer;
    private final SubscriptionResponseWrapperDeserializer<V> deserializer;

    public SubscriptionResponseWrapperSerde(final SubscriptionResponseWrapperSerializer<V> serializer,
                                            final SubscriptionResponseWrapperDeserializer<V> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<SubscriptionResponseWrapper<V>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<SubscriptionResponseWrapper<V>> deserializer() {
        return deserializer;
    }

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

}
