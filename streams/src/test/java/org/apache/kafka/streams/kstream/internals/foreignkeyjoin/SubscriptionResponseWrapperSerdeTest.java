/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Murmur3;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class SubscriptionResponseWrapperSerdeTest {

    @Test
    @SuppressWarnings("unchecked")
    public void nullForeignKeyTest(){
        long[] hashedValue = Murmur3.hash128(new byte[]{(byte)(0x01), (byte)(0x9A), (byte)(0xFF), (byte)(0x00)});
        SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, null, true);
        SubscriptionResponseWrapperSerde srwSerde = new SubscriptionResponseWrapperSerde(Serdes.String().serializer(), Serdes.String().deserializer());

        byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        SubscriptionResponseWrapper<String> result = (SubscriptionResponseWrapper<String>)srwSerde.deserializer().deserialize(null, serResponse);

        assertArrayEquals(hashedValue, result.getOriginalValueHash());
        assertEquals(null, result.getForeignValue());
        assertTrue(result.isPropagate());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void nonNullForeignKeyTest(){
        long[] hashedValue = Murmur3.hash128(new byte[]{(byte)(0x01), (byte)(0x9A), (byte)(0xFF), (byte)(0x00)});
        SubscriptionResponseWrapper<String> srw = new SubscriptionResponseWrapper<>(hashedValue, "foreignKey", true);
        SubscriptionResponseWrapperSerde srwSerde = new SubscriptionResponseWrapperSerde(Serdes.String().serializer(), Serdes.String().deserializer());

        byte[] serResponse = srwSerde.serializer().serialize(null, srw);
        SubscriptionResponseWrapper<String> result = (SubscriptionResponseWrapper<String>)srwSerde.deserializer().deserialize(null, serResponse);

        assertArrayEquals(hashedValue, result.getOriginalValueHash());
        assertEquals("foreignKey", result.getForeignValue());
        assertTrue(result.isPropagate());
    }
}
