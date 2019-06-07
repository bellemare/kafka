package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

class PropagationWrapperSerializer<V> implements Serializer<PropagationWrapper<V>> {

    private final Serializer<V> serializer;

    public PropagationWrapperSerializer(Serializer<V> serializer) {
        this.serializer = serializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Don't need to configure, they are already configured. This is just a wrapper.
    }

    @Override
    public byte[] serialize(String topic, PropagationWrapper<V> data) {
        //{8-byte long-offset}{byte boolean, stored in bit 0}{4-byte value length}{value}
        //1 byte
        byte printableOut = (byte)(data.isPropagate()?1:0);
        //8 bytes
        byte[] longOffset = Serdes.Long().serializer().serialize(topic, data.getOffset());

        if (data.getElem() != null) {
            //? bytes
            byte[] serializedData = serializer.serialize(topic, data.getElem());
            //4 bytes
            byte[] lengthSerializedData = numToBytes(serializedData.length);
            byte[] total = new byte[13 + serializedData.length];
            System.arraycopy(longOffset, 0, total, 0, 8);
            total[8] = printableOut;
            System.arraycopy(lengthSerializedData, 0, total, 9, 4);
            System.arraycopy(serializedData, 0, total, 13, serializedData.length);
            return total;
        } else {
            byte[] total = new byte[9];
            System.arraycopy(longOffset, 0, total, 0, 8);
            total[8] = printableOut;
            return total;
        }
    }

    private byte[] numToBytes(int num){
        ByteBuffer wrapped = ByteBuffer.allocate(4);
        wrapped.putInt(num);
        return wrapped.array();
    }

    @Override
    public void close() {
        this.serializer.close();
    }
}
