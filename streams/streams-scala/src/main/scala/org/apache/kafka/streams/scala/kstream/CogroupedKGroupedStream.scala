/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
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
package org.apache.kafka.streams.scala.kstream
package kstream

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{CogroupedKStream => CogroupedKStreamJ, _}
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Wraps the Java class KGroupedStream and delegates method calls to the underlying Java object.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 * @param inner The underlying Java abstraction for KGroupedStream
 *
 * @see `org.apache.kafka.streams.kstream.KGroupedStream`
 */
class CogroupedKGroupedStream[K, V](val inner: CogroupedKStreamJ[K, V]) {

  def cogroup[T](groupedStream: KGroupedStream[K, T], aggregator: Aggregator[_ >: K, _ >: T, V]): CogroupedKStreamJ[K, V] =
    inner.cogroup(groupedStream, aggregator)

  def aggregate[VR](initializer: Initializer[VR], valueSerde: Serde[VR]): KTable[K, VR] =
    inner.aggregate(initializer, valueSerde)

  def aggregate[VR](initializer: Initializer[VR], valueSerde: Serde[VR], materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]]): KTable[K, VR] =
    inner.aggregate(initializer, valueSerde, materialized)

  def windowedBy(windows: SessionWindows): SessionWindowedCogroupedKStream[K, V] =
    inner.windowedBy(windows)

}
