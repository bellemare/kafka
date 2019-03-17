package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionWrapperDeserializer implements Deserializer<SubscriptionWrapper> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Do nothing
    }

    @Override
    public SubscriptionWrapper deserialize(String topic, byte[] data) {
        //{16-bytes Hash}{1-byte boolean propagate}
        final ByteBuffer buf = ByteBuffer.wrap(data);
        final long[] hash = new long[2];
        hash[0] = buf.getLong();
        hash[1] = buf.getLong();
        boolean propagate = true;
        if (buf.get(16) == 0x00) {
            propagate = false;
        }
        return new SubscriptionWrapper(hash, propagate);
    }

    @Override
    public void close() {
        //Do nothing
    }
}
