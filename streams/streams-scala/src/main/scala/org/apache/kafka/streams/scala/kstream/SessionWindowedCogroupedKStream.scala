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

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{SessionWindowedCogroupedKStream => SessionWindowedCogroupedKStreamJ, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.SessionStore

/**
 * Wraps the Java class SessionWindowedCogroupedKStream and delegates method calls to the underlying Java object.
 *
 * @param [K] Type of keys
 * @param [V] Type of values
 * @param inner The underlying Java abstraction for SessionWindowedKStream
 *
 * @see `org.apache.kafka.streams.kstream.SessionWindowedKStream`
 */
class SessionWindowedCogroupedKStream[K, V](val inner: SessionWindowedCogroupedKStreamJ[K, V]) {
  def aggregate[VR](initializer: Initializer[VR], sessionMerger: Merger[_ >: K, VR], materialized: Materialized[K, VR, SessionStore[Bytes, Array[Byte]]]): KTable[Windowed[K], VR] =
    inner.aggregate(initializer, sessionMerger, materialized)
}
