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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDbTTLKeyValueBytesStoreSupplier;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Category({IntegrationTest.class})
public class AdamTest {
    private final static int NUM_BROKERS = 1;

    @ClassRule
    public final static EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final static String INPUT_TOPIC = "table1";
    private final static String OUTPUT = "output-";
    private static Properties streamsConfig;
    private KafkaStreams streams;

    @BeforeClass
    public static void beforeTest() throws Exception {
        //Use multiple partitions to ensure distribution of keys.
        CLUSTER.createTopic(INPUT_TOPIC, 3, 1);
        CLUSTER.createTopic(OUTPUT, 3, 1);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfig.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "myapplicationid");

        final Properties producerConfigOne = new Properties();
        producerConfigOne.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfigOne.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfigOne.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfigOne.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigOne.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<KeyValue<String, String>> table1 = Arrays.asList(
            new KeyValue<>("a", "aa"),
            new KeyValue<>("b", "aa"),
            new KeyValue<>("c", "aa"), //Won't be joined in yet.
            new KeyValue<>("c", "aa")  //Won't be joined in at all.
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_TOPIC, table1, producerConfigOne, Time.SYSTEM);
    }

    @Before
    public void before() throws IOException {
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @After
    public void after() throws IOException {
        if (streams != null) {
            streams.close();
            streams = null;
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfig);
    }

    @Test
    public void doTest() throws IOException {
        String tableName = "queryableName";
        KafkaStreams topology = prepareTopology(tableName);
        topology.start();

        try {
            System.out.println("Sleeping for 20 seconds");
            Thread.sleep(20000);
            ReadOnlyKeyValueStore<String, Long> keystore = topology.store(tableName, QueryableStoreTypes.keyValueStore());
            System.out.println("a = " + keystore.get("a"));
            System.out.println("b = " + keystore.get("b"));
            System.out.println("c = " + keystore.get("c"));
            Thread.sleep(1000);
            topology.close();
            System.out.println("Sleeping for 55 seconds");
            Thread.sleep(55000);

            System.out.println("Starting up the topology again to restore the state store");
            KafkaStreams topology2 = prepareTopology(tableName);

            Thread.sleep(5000);
            topology2.start();

            System.out.println("Let it stabilize for 20 seconds");
            Thread.sleep(20000);

            System.out.println("Sleeping for 10 seconds");
            ReadOnlyKeyValueStore<String, Long> keystore2 = topology2.store(tableName, QueryableStoreTypes.keyValueStore());
            System.out.println("a = " + keystore2.get("a"));
            System.out.println("b = " + keystore2.get("b"));
            System.out.println("c = " + keystore2.get("c"));

            Thread.sleep(2000);
            System.out.println("Done.");
        } catch (Exception e) {
            System.out.println("HEXCEPTION - Ah damnit.");
            System.out.println(e.toString());
        }
    }


    private KafkaStreams prepareTopology(final String queryableName) {
        final StreamsBuilder builder = new StreamsBuilder();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafkastreams/myapplicationid" + System.currentTimeMillis());

        RocksDbTTLKeyValueBytesStoreSupplier supplier = new RocksDbTTLKeyValueBytesStoreSupplier(queryableName, 60);

        builder
            .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(supplier).withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfig);
    }
}
