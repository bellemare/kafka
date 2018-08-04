package org.apache.kafka.streams.kstream.internals.onetomany;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Factory for creating CombinedKey serializers / deserializers.
 */
public class CombinedKeySerde<KL, KR> implements Serde<CombinedKey<KL, KR>> {

    //static protected class WrapperSerde<KL, KR> implements Serde<CombinedKey<KL, KR>> {
        final private Serializer<KR> rightSerializer;
        final private Deserializer<KR> rightDeserializer;
        final private Serializer<KL> leftSerializer;
        final private Deserializer<KL> leftDeserializer;
        final private Serializer<CombinedKey<KL,KR>> serializer;
        final private Deserializer<CombinedKey<KL,KR>> deserializer;


    //    public WrapperSerde(Serializer<KL> leftSerializer, Deserializer<KL> leftDeserializer,
//                        Serializer<KR> rightSerializer, Deserializer<KR> rightDeserializer) {
        public CombinedKeySerde(Serde<KL> leftSerde, Serde<KR> rightSerde) {
            this.rightSerializer = rightSerde.serializer();
            this.rightDeserializer = rightSerde.deserializer();
            this.leftDeserializer = leftSerde.deserializer();
            this.leftSerializer = leftSerde.serializer();
            this.serializer = new CombinedKeySerializer<>(leftSerializer, rightSerializer);
            this.deserializer = new CombinedKeyDeserializer<>(leftDeserializer, rightDeserializer);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            rightSerializer.configure(configs, isKey);
            leftSerializer.configure(configs,isKey);
            rightDeserializer.configure(configs,isKey);
            leftDeserializer.configure(configs,isKey);
        }

        @Override
        public void close() {
            rightDeserializer.close();
            leftDeserializer.close();
            rightSerializer.close();
            leftSerializer.close();
        }

        @Override
        public Serializer<CombinedKey<KL,KR>> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<CombinedKey<KL,KR>> deserializer() {
            return deserializer;
        }

        public Serializer<KL> getLeftSerializer() {
            return this.leftSerializer;
        }
    //}



}
