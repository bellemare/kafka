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

package org.apache.kafka.streams.kstream.internals.graph;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

public class OptimizableRepartitionNode<K, V, RepartitionKey, RepartitionValue> extends BaseRepartitionNode<RepartitionKey, RepartitionValue> {

    public OptimizableRepartitionNode(final String nodeName,
                               final String sourceName,
                               final ProcessorParameters<K, V> processorParameters,
                               final Serde<RepartitionKey> keySerde,
                               final Serde<RepartitionValue> valueSerde,
                               final String sinkName,
                               final String repartitionTopic) {

        super(
            nodeName,
            sourceName,
            processorParameters,
            keySerde,
            valueSerde,
            sinkName,
            repartitionTopic
        );
    }

    public Serde<RepartitionKey> keySerde() {
        return keySerde;
    }

    public Serde<RepartitionValue> valueSerde() {
        return valueSerde;
    }

    public String repartitionTopic() {
        return repartitionTopic;
    }

    @Override
    Serializer<RepartitionValue> getValueSerializer() {
        return valueSerde != null ? valueSerde.serializer() : null;
    }

    @Override
    Deserializer<RepartitionValue> getValueDeserializer() {
        return  valueSerde != null ? valueSerde.deserializer() : null;
    }

    @Override
    public String toString() {
        return "OptimizableRepartitionNode{ " + super.toString() + " }";
    }

    @Override
    public void writeToTopology(final InternalTopologyBuilder topologyBuilder) {
        final Serializer<RepartitionKey> keySerializer = keySerde != null ? keySerde.serializer() : null;
        final Deserializer<RepartitionKey> keyDeserializer = keySerde != null ? keySerde.deserializer() : null;

        topologyBuilder.addInternalTopic(repartitionTopic);

        topologyBuilder.addProcessor(
            processorParameters.processorName(),
            processorParameters.processorSupplier(),
            parentNodeNames()
        );

        topologyBuilder.addSink(
            sinkName,
            repartitionTopic,
            keySerializer,
            getValueSerializer(),
            null,
            processorParameters.processorName()
        );

        topologyBuilder.addSource(
            null,
            sourceName,
            new FailOnInvalidTimestamp(),
            keyDeserializer,
            getValueDeserializer(),
            repartitionTopic
        );

    }

    public static <K, V, RepartitionKey, RepartitionValue> OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> optimizableRepartitionNodeBuilder() {
        return new OptimizableRepartitionNodeBuilder<>();
    }


    public static final class OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> {

        private String nodeName;
        private ProcessorParameters<K, V> processorParameters;
        private Serde<RepartitionKey> keySerde;
        private Serde<RepartitionValue> valueSerde;
        private String sinkName;
        private String sourceName;
        private String repartitionTopic;

        private OptimizableRepartitionNodeBuilder() {
        }

        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withProcessorParameters(final ProcessorParameters<K, V> processorParameters) {
            this.processorParameters = processorParameters;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withKeySerde(final Serde<RepartitionKey> keySerde) {
            this.keySerde = keySerde;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withValueSerde(final Serde<RepartitionValue> valueSerde) {
            this.valueSerde = valueSerde;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withSinkName(final String sinkName) {
            this.sinkName = sinkName;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withSourceName(final String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withRepartitionTopic(final String repartitionTopic) {
            this.repartitionTopic = repartitionTopic;
            return this;
        }


        public OptimizableRepartitionNodeBuilder<K, V, RepartitionKey, RepartitionValue> withNodeName(final String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public OptimizableRepartitionNode<K, V, RepartitionKey, RepartitionValue> build() {

            return new OptimizableRepartitionNode<>(
                nodeName,
                sourceName,
                processorParameters,
                keySerde,
                valueSerde,
                sinkName,
                repartitionTopic
            );

        }
    }
}
