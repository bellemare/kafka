package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Factory for creating CombinedKey serializers / deserializers.
 */
public class PropagationWrapperSerde<V> implements Serde<PropagationWrapper<V>> {
    final private Serializer<PropagationWrapper<V>> serializer;
    final private Deserializer<PropagationWrapper<V>> deserializer;

    public PropagationWrapperSerde(Serde<V> serde) {
        this.serializer = new PropagationWrapperSerializer<>(serde.serializer());
        this.deserializer = new PropagationWrapperDeserializer<>(serde.deserializer());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs,isKey);
        deserializer.configure(configs,isKey);
    }

    @Override
    public void close() {
        deserializer.close();
        serializer.close();
    }

    @Override
    public Serializer<PropagationWrapper<V>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<PropagationWrapper<V>> deserializer() {
        return deserializer;
    }

}
