package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

class PropagationWrapperDeserializer<V> implements Deserializer<PropagationWrapper<V>> {

    private final Deserializer<V> deserializer;

    public PropagationWrapperDeserializer(Deserializer<V> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Don't need to configure them, as they are already configured. This is only a wrapper.
    }

    @Override
    public PropagationWrapper<V> deserialize(String topic, byte[] data) {
        //{byte boolean, stored in bit 0}{4-byte value length}{value}

        byte[] printableLengthRaw = Arrays.copyOfRange(data, 0, 1);
        BitSet bits = BitSet.valueOf(printableLengthRaw);
        boolean printable = bits.get(0);

        V value = null;
        if (data.length >= 5) {
            byte[] count = Arrays.copyOfRange(data,1,5);
            int offset = ByteBuffer.wrap(count).getInt();
            byte[] rawVal = Arrays.copyOfRange(data,5,5+offset);
            value = deserializer.deserialize(topic, rawVal);
        }

        return new PropagationWrapper<>(value, printable);
    }

    @Override
    public void close() {
        deserializer.close();
    }

}
