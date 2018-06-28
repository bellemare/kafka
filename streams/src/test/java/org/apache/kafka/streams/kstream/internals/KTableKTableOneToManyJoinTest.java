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
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsBuilderTest;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KTableKTableOneToManyJoinTest {

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";

    private final Serde<String> stringSerde = Serdes.String();
    private final Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
    //private final Materialized<Integer, String, KeyValueStore<Bytes, byte[]>> materialized = Materialized.with(intSerde, stringSerde);

    private File stateDir = null;
    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Before
    public void setUp() {
        stateDir = TestUtils.tempDirectory("kafka-test");
    }

    private void doTestJoin(final StreamsBuilder builder,
                            final String[] expectedKeys,
                            final MockProcessorSupplier<String, String> supplier,
                            final KTable<String, String> joined) {

        final Collection<Set<String>> copartitionGroups = StreamsBuilderTest.getCopartitionedGroups(builder);

//        final Collection<Set<String>> copartitionGroups = TopologyWrapper.getInternalTopologyBuilder(builder.build())
//                .setApplicationId("foobarSomeAppId")
//                .copartitionGroups();

        //assertEquals(1, copartitionGroups.size()); //TODO What is this even testing?
        //assertEquals(new HashSet<>(Arrays.asList(topic1, topic2)), copartitionGroups.iterator().next());

        final KTableValueGetterSupplier<String, String> getterSupplier = ((KTableImpl<String, String, String>) joined).valueGetterSupplier();

        driver.setUp(builder, stateDir, Serdes.String(), Serdes.String());
        driver.setTime(0L);

        final KTableValueGetter<String, String> getter = getterSupplier.get();
        getter.init(driver.context());


        for (StateStore o: driver.allStateStores().values()) {
            System.out.println("The state store is = " + o.name() + ", type = " + o.toString());
        }

        for (int i = 0; i < 2; i++) {
            driver.process(topic1, expectedKeys[i], expectedKeys[i] + ",X");
            System.out.println("Table1-row = (" + expectedKeys[i] + ", " + expectedKeys[i] + ",X)" );
        }
        // pass tuple with null key, it will be discarded in join process
        //driver.process(topic1, null, "SomeVal");
        driver.flushState();


        for (int i = 5; i < 8; i++) {
            driver.process(topic2, String.valueOf(i), "1,"+i+",YYYY");
            System.out.println("Table2-row = (" + String.valueOf(i) + ", 1,"+i+",YYYY)" );
        }
        // pass tuple with null key, it will be discarded in join process
        //driver.process(topic2, null, "AnotherVal");
        driver.flushState();

        supplier.checkAndClearProcessResult("5:value1=1,X,value2=1,5,YYYY", "6:value1=1,X,value2=1,6,YYYY", "7:value1=1,X,value2=1,7,YYYY");

        checkJoinedValues(getter, kv("5", "value1=1,X,value2=1,5,YYYY"), kv("6", "value1=1,X,value2=1,6,YYYY"), kv("7","value1=1,X,value2=1,7,YYYY"));

    }

    @Test
    public void testJoin() {
        final StreamsBuilder builder = new StreamsBuilder();

        final String[] expectedKeys = new String[]{"0", "1", "2", "3"};

        final KTable<String, String> table1;
        final KTable<String, String> table2;
        final KTable<String, String> joined;
        final MockProcessorSupplier<String, String> supplier = new MockProcessorSupplier<>();
        table1 = builder.table(topic1, consumed);
        table2 = builder.table(topic2, consumed);


        /*
        <K0, V0, KO, VO> KTable<K0, V0> oneToManyJoin(KTable<KO, VO> other,
			ValueMapper<VO, K> keyExtractor,
			ValueMapper<K, K0> joinPrefixFaker,
			ValueMapper<K0, K> leftKeyExtractor,
			ValueJoiner<V, VO, V0> joiner,
			Serde<KO> keyOtherSerde,
			Serde<VO> valueOtherSerde,
			Serde<K0> joinKeySerde,
			Serde<V0> joinValueSerde);
         */

        //One is on the left
        //Many is on the right.
        //Rekey the many on the right to join the one on the left.

        //Remaps the left key to the correct partitions, to be on the same node as the right keys.
        //Step 1: Repartition left key data. Get the foreign key out of the value!
        ValueMapper<String, String> keyExtractor = new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                //Assuming format of: "foreignKey,primaryKey,metadata"
                String[] ss = value.split(",");
                System.out.println("Extracted data: " + ss);
                return ss[0] + "-" + ss[1];
            }
        };

        //Use the LEFT key to generate the RIGHT prefix. -> Straight up getter?? urgh.
        ValueMapper<String, String> joinPrefixFaker = new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                System.out.println("joinPrefixFaker value = " + value);
                return value;
            }
        };

        //I think this gets the left key out of the whole joined key experience...
        //ie: "leftKeyForeign-rightKeyPrimary" : "someValue"
        //Used to repartition the data correctly.
        ValueMapper<String, String> leftKeyExtractor = new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                //Assumes key format of foreign-primary.
                String[] ss = value.split("-");
                System.out.println("leftKeyExtractor = " + ss);
                return ss[0];
            }
        };

        //I think this gets the left key out of the whole joined key experience...
        //ie: "leftKeyForeign-rightKeyPrimary" : "someValue"
        //Used to repartition the data correctly.
        ValueMapper<String, String> rightKeyExtractor = new ValueMapper<String, String>() {
            @Override
            public String apply(String value) {
                //Assumes key format of foreign-primary.
                String[] ss = value.split("-");
                System.out.println("leftKeyExtractor = " + ss);
                return ss[1];
            }
        };

        //TODO - Properly merge the string output.
        ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return "value1=" + value1 + ",value2=" + value2;
            }
        };

        joined = table1
                .oneToManyJoin(table2, keyExtractor, joinPrefixFaker, leftKeyExtractor, rightKeyExtractor, joiner, Serdes.String(), Serdes.String(), Serdes.String(), Serdes.String());

        //Load the process supplier for the test.
        joined.toStream().process(supplier);

        doTestJoin(builder, expectedKeys, supplier, joined);
    }

//    @Test
//    public void testQueryableJoin() {
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        final int[] expectedKeys = new int[]{0, 1, 2, 3};
//
//        final KTable<Integer, String> table1;
//        final KTable<Integer, String> table2;
//        final KTable<Integer, String> table3;
//        final MockProcessorSupplier<Integer, String> processor;
//
//        processor = new MockProcessorSupplier<>();
//        table1 = builder.table(topic1, consumed);
//        table2 = builder.table(topic2, consumed);
//        table3 = table1.join(table2, MockValueJoiner.TOSTRING_JOINER, materialized);
//        table3.toStream().process(processor);
//
//        doTestJoin(builder, expectedKeys, processor, table3);
//    }
//
//    private void doTestSendingOldValues(final StreamsBuilder builder,
//                                        final int[] expectedKeys,
//                                        final KTable<Integer, String> table1,
//                                        final KTable<Integer, String> table2,
//                                        final MockProcessorSupplier<Integer, String> supplier,
//                                        final KTable<Integer, String> joined,
//                                        final boolean sendOldValues) {
//
//        driver.setUp(builder, stateDir, Serdes.Integer(), Serdes.String());
//        driver.setTime(0L);
//
//        final MockProcessor<Integer, String> proc = supplier.theCapturedProcessor();
//
//        if (!sendOldValues) {
//            assertFalse(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
//            assertFalse(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
//            assertFalse(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());
//        } else {
//            ((KTableImpl<?, ?, ?>) joined).enableSendingOldValues();
//            assertTrue(((KTableImpl<?, ?, ?>) table1).sendingOldValueEnabled());
//            assertTrue(((KTableImpl<?, ?, ?>) table2).sendingOldValueEnabled());
//            assertTrue(((KTableImpl<?, ?, ?>) joined).sendingOldValueEnabled());
//        }
//        // push two items to the primary stream. the other table is empty
//
//        for (int i = 0; i < 2; i++) {
//            driver.process(topic1, expectedKeys[i], "X" + expectedKeys[i]);
//        }
//        driver.flushState();
//
//        proc.checkAndClearProcessResult();
//
//        // push two items to the other stream. this should produce two items.
//
//        for (int i = 0; i < 2; i++) {
//            driver.process(topic2, expectedKeys[i], "Y" + expectedKeys[i]);
//        }
//        driver.flushState();
//
//        proc.checkAndClearProcessResult("0:(X0+Y0<-null)", "1:(X1+Y1<-null)");
//
//        // push all four items to the primary stream. this should produce two items.
//
//        for (final int expectedKey : expectedKeys) {
//            driver.process(topic1, expectedKey, "XX" + expectedKey);
//        }
//        driver.flushState();
//
//        proc.checkAndClearProcessResult("0:(XX0+Y0<-null)", "1:(XX1+Y1<-null)");
//
//        // push all items to the other stream. this should produce four items.
//        for (final int expectedKey : expectedKeys) {
//            driver.process(topic2, expectedKey, "YY" + expectedKey);
//        }
//        driver.flushState();
//        proc.checkAndClearProcessResult("0:(XX0+YY0<-null)", "1:(XX1+YY1<-null)", "2:(XX2+YY2<-null)", "3:(XX3+YY3<-null)");
//
//        // push all four items to the primary stream. this should produce four items.
//
//        for (final int expectedKey : expectedKeys) {
//            driver.process(topic1, expectedKey, "X" + expectedKey);
//        }
//        driver.flushState();
//        proc.checkAndClearProcessResult("0:(X0+YY0<-null)", "1:(X1+YY1<-null)", "2:(X2+YY2<-null)", "3:(X3+YY3<-null)");
//
//        // push two items with null to the other stream as deletes. this should produce two item.
//
//        for (int i = 0; i < 2; i++) {
//            driver.process(topic2, expectedKeys[i], null);
//        }
//        driver.flushState();
//        proc.checkAndClearProcessResult("0:(null<-null)", "1:(null<-null)");
//
//        // push all four items to the primary stream. this should produce two items.
//
//        for (final int expectedKey : expectedKeys) {
//            driver.process(topic1, expectedKey, "XX" + expectedKey);
//        }
//        driver.flushState();
//        proc.checkAndClearProcessResult("2:(XX2+YY2<-null)", "3:(XX3+YY3<-null)");
//    }
//
//    @Test
//    public void testNotSendingOldValues() {
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        final int[] expectedKeys = new int[]{0, 1, 2, 3};
//
//        final KTable<Integer, String> table1;
//        final KTable<Integer, String> table2;
//        final KTable<Integer, String> joined;
//        final MockProcessorSupplier<Integer, String> supplier;
//
//        table1 = builder.table(topic1, consumed);
//        table2 = builder.table(topic2, consumed);
//        joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER);
//        supplier = new MockProcessorSupplier<>();
//        builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);
//
//        doTestSendingOldValues(builder, expectedKeys, table1, table2, supplier, joined, false);
//
//    }
//
//    @Test
//    public void testQueryableNotSendingOldValues() {
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        final int[] expectedKeys = new int[]{0, 1, 2, 3};
//
//        final KTable<Integer, String> table1;
//        final KTable<Integer, String> table2;
//        final KTable<Integer, String> joined;
//        final MockProcessorSupplier<Integer, String> supplier;
//
//        table1 = builder.table(topic1, consumed);
//        table2 = builder.table(topic2, consumed);
//        joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER, materialized);
//        supplier = new MockProcessorSupplier<>();
//        builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);
//
//        doTestSendingOldValues(builder, expectedKeys, table1, table2, supplier, joined, false);
//    }
//
//    @Test
//    public void testSendingOldValues() {
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        final int[] expectedKeys = new int[]{0, 1, 2, 3};
//
//        final KTable<Integer, String> table1;
//        final KTable<Integer, String> table2;
//        final KTable<Integer, String> joined;
//        final MockProcessorSupplier<Integer, String> supplier;
//
//        table1 = builder.table(topic1, consumed);
//        table2 = builder.table(topic2, consumed);
//        joined = table1.join(table2, MockValueJoiner.TOSTRING_JOINER);
//
//        supplier = new MockProcessorSupplier<>();
//        builder.build().addProcessor("proc", supplier, ((KTableImpl<?, ?, ?>) joined).name);
//
//        doTestSendingOldValues(builder, expectedKeys, table1, table2, supplier, joined, true);
//
//    }
//
//    @Test
//    public void shouldLogAndMeterSkippedRecordsDueToNullLeftKey() {
//        final StreamsBuilder builder = new StreamsBuilder();
//
//        final Processor<String, Change<String>> join = new KTableKTableInnerJoin<>(
//            (KTableImpl<String, String, String>) builder.table("left", Consumed.with(stringSerde, stringSerde)),
//            (KTableImpl<String, String, String>) builder.table("right", Consumed.with(stringSerde, stringSerde)),
//            null
//        ).get();
//
//        final MockProcessorContext context = new MockProcessorContext();
//        context.setRecordMetadata("left", -1, -2, null, -3);
//        join.init(context);
//        final LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//        join.process(null, new Change<>("new", "old"));
//        LogCaptureAppender.unregister(appender);
//
//        assertEquals(1.0, getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
//        assertThat(appender.getMessages(), hasItem("Skipping record due to null key. change=[(new<-old)] topic=[left] partition=[-1] offset=[-2]"));
//    }

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