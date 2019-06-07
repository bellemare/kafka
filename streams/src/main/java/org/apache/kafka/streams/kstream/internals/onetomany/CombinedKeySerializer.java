package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Serializer;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.util.Map;

class CombinedKeySerializer<KL,KR> implements Serializer<CombinedKey<KL,KR>> {

    private final Serializer<KL> leftSerializer;
    private final Serializer<KR> rightSerializer;

    public CombinedKeySerializer(Serializer<KL> leftSerializer, Serializer<KR> rightSerializer) {
        this.leftSerializer = leftSerializer;
        this.rightSerializer = rightSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //Don't need to configure, they are already configured. This is just a wrapper.
    }

    @Override
    public byte[] serialize(String topic, CombinedKey<KL, KR> data) {
        //{4-byte left length}{leftKeySerialized}{4-byte right length}{rightKeySerialized}

        //? bytes
        byte[] leftSerializedData = leftSerializer.serialize(topic, data.getForeignKey());
        //4 bytes
        byte[] leftSize = numToBytes(leftSerializedData.length);

        if (data.getPrimaryKey() != null) {
            //? bytes
            byte[] rightSerializedData = rightSerializer.serialize(topic, data.getPrimaryKey());
            //4 bytes
            byte[] rightSize = numToBytes(rightSerializedData.length);
            byte[] total = new byte[8 + leftSerializedData.length + rightSerializedData.length];
            System.arraycopy(leftSize, 0, total, 0, 4);
            System.arraycopy(leftSerializedData, 0, total, 4, leftSerializedData.length);
            System.arraycopy(rightSize, 0, total, 4+leftSerializedData.length, 4);
            System.arraycopy(rightSerializedData, 0, total, 8+leftSerializedData.length, rightSerializedData.length);
            return total;
        } else {
            byte[] total = new byte[4 + leftSerializedData.length];
            System.arraycopy(leftSize, 0, total, 0, 4);
            System.arraycopy(leftSerializedData, 0, total, 4, leftSerializedData.length);
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
        this.leftSerializer.close();
        this.rightSerializer.close();
    }
}
