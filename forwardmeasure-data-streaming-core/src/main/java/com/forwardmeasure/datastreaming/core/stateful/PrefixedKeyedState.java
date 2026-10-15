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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * One node's view of a shared store: its keys carry the node's prefix, so every node of a graph can
 * live in one store and a record's changes to all of them commit together.
 */
public final class PrefixedKeyedState implements KeyedState {

  private static final char SEPARATOR = '\u0002';

  private final String prefix;
  private final KeyedState shared;

  public PrefixedKeyedState(String node, KeyedState shared) {
    this.prefix = node + SEPARATOR;
    this.shared = shared;
  }

  @Override
  public Optional<String> get(String key) {
    return shared.get(prefix + key);
  }

  @Override
  public void put(String key, String value) {
    shared.put(prefix + key, value);
  }

  @Override
  public void remove(String key) {
    shared.remove(prefix + key);
  }

  @Override
  public Iterable<String> keys() {
    List<String> keys = new ArrayList<>();
    for (String key : shared.keys()) {
      if (key.startsWith(prefix)) keys.add(key.substring(prefix.length()));
    }
    return keys;
  }
}
