package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class SubscriptionWrapperSerde implements Serde {
    private final SubscriptionWrapperSerializer serializer;
    private final SubscriptionWrapperDeserializer deserializer;

    public SubscriptionWrapperSerde(final SubscriptionWrapperSerializer serializer,
                                    final SubscriptionWrapperDeserializer deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public SubscriptionWrapperSerde() {
        this.serializer = new SubscriptionWrapperSerializer();
        this.deserializer = new SubscriptionWrapperDeserializer();
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return this.serializer;
    }

    @Override
    public Deserializer deserializer() {
        return this.deserializer;
    }
}
