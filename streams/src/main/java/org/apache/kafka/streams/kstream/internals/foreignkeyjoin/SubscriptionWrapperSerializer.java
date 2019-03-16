package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class SubscriptionWrapperSerializer implements Serializer<SubscriptionWrapper> {
    public SubscriptionWrapperSerializer() {
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        //Don't need to configure, they are already configured. This is just a wrapper.
    }

    @Override
    public byte[] serialize(String topic, SubscriptionWrapper data) {
        //{16-bytes Hash}{1-byte boolean propagate}
        final ByteBuffer buf = ByteBuffer.allocate(17);
        buf.put(data.getHash());
        buf.put((byte) (data.isPropagate() ? 1 : 0 ));
        return buf.array();
    }

    @Override
    public void close() {

    }
}
