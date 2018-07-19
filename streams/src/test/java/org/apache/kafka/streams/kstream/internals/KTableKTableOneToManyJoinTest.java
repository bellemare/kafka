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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsBuilderTest;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKey;
import org.apache.kafka.streams.kstream.internals.onetomany.CombinedKeySerde;
import org.apache.kafka.streams.kstream.internals.onetomany.PropagationWrapper;
import org.apache.kafka.streams.kstream.internals.onetomany.PropagationWrapperSerde;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableKTableOneToManyJoinTest {

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final String topic3 = "topic3";

    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);

    private File stateDir = null;

    private static final String APP_ID = "app-id";

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String[] expectedKeys = new String[]{"0", "1", "2", "3"};

        final KTable<String, String> table1;
        final KTable<String, String> table2;
        final KTable<String, String> table3;
        final KTable<String, String> joined;
        final KTable<String, String> joined2;
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        final MockProcessorSupplier<String, String> supplierTwo = new MockProcessorSupplier<>();
        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);
        table3 = builder.table(topic3, consumed);

        //One is on the left
        //Many is on the right.
        //Rekey the many on the right to join the one on the left.
        ValueMapper<String, String> tableOneKeyExtractor = new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                //Assuming format of: "foreignKey,metadata"
                String[] ss = value.split(",");
                return ss[0];
            }
        };

        ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return "value1=" + value1 + ",value2=" + value2;
            }
        };

        CombinedKey<String, Double> foo = new CombinedKey<>("eat pant. eat all the pant. only eat pant.", 24.23423161346);

        CombinedKeySerde<String, Double> ff = new CombinedKeySerde<>(Serdes.String(), Serdes.Double());

        byte[] ss = ff.serializer().serialize("dummyTopic", foo);
        CombinedKey<String, Double> gg = ff.deserializer().deserialize("dummyTopic", ss);

        Materialized<String, String, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("SomeStore")
                .withCachingDisabled()
                .withLoggingDisabled()
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String());

        Materialized<String, String, KeyValueStore<Bytes, byte[]>> mat2 =
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("SomeStore2")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String());


        PropagationWrapperSerde<String> pwSerde = new PropagationWrapperSerde(Serdes.String());
        PropagationWrapper<String> fooWrap = new PropagationWrapper<>("my string", true, 100L);

        byte[] result = pwSerde.serializer().serialize("someTopic", fooWrap);
        PropagationWrapper<String> unwrappedFoo = pwSerde.deserializer().deserialize("someTopic", result);
        System.out.println("elem = " + unwrappedFoo.getElem() + ", boolean = " + unwrappedFoo.isPrintable());
        System.out.println("elem = " + fooWrap.getElem() + ", boolean = " + fooWrap.isPrintable());
        assertEquals(unwrappedFoo.isPrintable(), fooWrap.isPrintable());
        assertEquals(unwrappedFoo.getElem(), fooWrap.getElem());

        PropagationWrapper<String> nullWrap = new PropagationWrapper<>(null, true, 100L);

        byte[] nullWrapResult = pwSerde.serializer().serialize("someTopic", nullWrap);
        PropagationWrapper<String> someUnwrappedNullResult = pwSerde.deserializer().deserialize("someTopic", nullWrapResult);

        System.out.println(someUnwrappedNullResult);

        joined = table1
                .oneToManyJoin(table2, tableOneKeyExtractor, joiner, mat, Serdes.String(), Serdes.String(), Serdes.String(), Serdes.String());

        joined2 = joined
                .oneToManyJoin(table3, tableOneKeyExtractor, joiner, mat2, Serdes.String(), Serdes.String(), Serdes.String(), Serdes.String());

        //Load the process supplier for the test.
        joined.toStream().process(supplier);
        joined2.toStream().process(supplierTwo);

        doTestJoin(builder, expectedKeys, supplier, supplierTwo, joined, joined2);
    }


    private void doTestJoin(final StreamsBuilder builder,
                            final String[] expectedKeys,
                            final MockProcessorSupplier<String, String> supplier,
                            final MockProcessorSupplier<String, String> supplier2,
                            final KTable<String, String> joined,
                            final KTable<String, String> joined2) {

        StreamsBuilderTest.internalTopologyBuilder(builder).setApplicationId(APP_ID);

        final Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

        //This ensures that there is a copartition between the repartitioned right table and the left table.

        Iterator f = copartitionGroups.iterator();
        while (f.hasNext())
            System.out.println(f.next());

        //assertEquals(1, copartitionGroups.size());



        final KTableValueGetterSupplier<String, String> getterSupplier = ((KTableImpl<String, String, String>) joined).valueGetterSupplier();

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.String());
        driver.setTime(0L);

        final KTableValueGetter<String, String> getter = getterSupplier.get();
        getter.init(driver.context());

        for (int i = 0; i < 3; i++) {
            driver.process(topic1, expectedKeys[i], expectedKeys[i] + ",X");
        }
        // pass tuple with null key, it will be discarded in join process
        driver.process(topic1, null, "SomeVal");
        driver.flushState();

        for (int i = 5; i < 8; i++) {
            driver.process(topic2, String.valueOf(i), "1,"+i+",YYYY");
        }
        // pass tuple with null key, it will be discarded in join process
        driver.process(topic2, null, "AnotherVal");
        driver.flushState();

        supplier.checkAndClearProcessResult("5:value1=1,X,value2=1,5,YYYY", "6:value1=1,X,value2=1,6,YYYY", "7:value1=1,X,value2=1,7,YYYY");

        checkJoinedValues(getter, kv("5", "value1=1,X,value2=1,5,YYYY"), kv("6", "value1=1,X,value2=1,6,YYYY"), kv("7","value1=1,X,value2=1,7,YYYY"));

        //Now update from the other side.
        driver.process(topic1, "1", "1,XYZ");
        driver.flushState();

        supplier.checkAndClearProcessResult("5:value1=1,XYZ,value2=1,5,YYYY", "6:value1=1,XYZ,value2=1,6,YYYY", "7:value1=1,XYZ,value2=1,7,YYYY");
        checkJoinedValues(getter, kv("5", "value1=1,XYZ,value2=1,5,YYYY"), kv("6", "value1=1,XYZ,value2=1,6,YYYY"), kv("7","value1=1,XYZ,value2=1,7,YYYY"));

        for (int i = 12; i < 13; i++) {
            driver.process(topic3, String.valueOf(i), "6,"+i+",ZZZZ");
        }
        driver.flushState();

        supplier2.checkAndClearProcessResult("12:value1=value1=1,XYZ,value2=1,6,YYYY,value2=6,12,ZZZZ");
    }

    private KeyValue<String, String> kv(final String key, final String value) {
        return new KeyValue<>(key, value);
    }

    @SafeVarargs
    private final void checkJoinedValues(final KTableValueGetter<String, String> getter, final KeyValue<String, String>... expected) {
        for (final KeyValue<String, String> kv : expected) {
            final String value = getter.get(kv.key);
            if (kv.value == null) {
                assertNull(value);
            } else {
                assertEquals(kv.value, value);
            }
        }
    }
}