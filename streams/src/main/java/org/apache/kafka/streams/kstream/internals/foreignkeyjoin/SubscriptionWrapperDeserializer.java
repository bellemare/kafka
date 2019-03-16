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
        final byte[] hash = new byte[16];
        buf.get(hash, 0, 16);
        boolean propagate = true;
        if (buf.get(16) == 0x00) {
            propagate = false;
        }
        return new SubscriptionWrapper(hash, propagate);

//        if (tempInst == 0x00)
//            instruction = SubscriptionInstruction.DELETE_AND_PROPAGATE;
//        else if (tempInst == 0x01)
//            instruction = SubscriptionInstruction.DELETE_NO_PROPAGATE;
//        else
//            instruction = SubscriptionInstruction.PROPAGATE;
//
//        return new SubscriptionWrapper(hash, instruction);

    }

    @Override
    public void close() {
        //Do nothing
    }
}
