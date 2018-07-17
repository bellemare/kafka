package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        //{byte boolean, stored in bit 0}{4-byte value length}{value}

        byte printableOut = (byte)(data.isPrintable()?1:0);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try {
            output.write(printableOut);
            if (data != null) {
                byte[] serializedData = serializer.serialize(topic, data.getElem());
                if (serializedData != null) {
                    byte[] lengthSerializedData = numToBytes(serializedData.length);
                    output.write(lengthSerializedData);
                    output.write(serializedData);
                }
            }
        } catch (IOException e){
            //TODO - Bellemare - yech. Handle the IO exception without passing it up.. ha.
            //System.out.println("IOException while handling serialization of CombinedKey " + e.toString());
        }







        return output.toByteArray();
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
