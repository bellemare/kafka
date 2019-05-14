package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

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

}
