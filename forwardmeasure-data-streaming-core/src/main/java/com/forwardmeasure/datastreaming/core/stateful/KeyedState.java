/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.datastreaming.core.stateful;

import java.util.Optional;

/**
 * One graph node's keyed state, as JSON text per key - the store each delivery engine provides: a
 * Kafka Streams key-value store (RocksDB with a changelog topic) or Pekko Persistence on the
 * installation's journal. Iteration is in key order, so every engine emits in the same order.
 */
public interface KeyedState {
  Optional<String> get(String key);

  void put(String key, String value);

  void remove(String key);

  /** Every key, in ascending order. */
  Iterable<String> keys();
}
