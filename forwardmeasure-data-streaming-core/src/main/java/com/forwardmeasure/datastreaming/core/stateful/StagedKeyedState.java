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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Writes held back until {@link #commit()}: a continuous engine processes a record, writes its
 * outputs to the sink, and only then commits the record's state changes, so a crash between the two
 * replays the record against the old state instead of losing its outputs.
 */
public final class StagedKeyedState implements KeyedState {

  /** One staged change; a {@code null} value removes the key. */
  public record Change(String key, String value) {}

  private final KeyedState committed;
  private final Map<String, Optional<String>> staged = new LinkedHashMap<>();

  public StagedKeyedState(KeyedState committed) {
    this.committed = committed;
  }

  @Override
  public Optional<String> get(String key) {
    Optional<String> pending = staged.get(key);
    return pending != null ? pending : committed.get(key);
  }

  @Override
  public void put(String key, String value) {
    staged.put(key, Optional.of(value));
  }

  @Override
  public void remove(String key) {
    staged.put(key, Optional.empty());
  }

  @Override
  public Iterable<String> keys() {
    TreeSet<String> keys = new TreeSet<>();
    committed.keys().forEach(keys::add);
    staged.forEach(
        (key, value) -> {
          if (value.isPresent()) keys.add(key);
          else keys.remove(key);
        });
    return List.copyOf(keys);
  }

  /** The changes staged since the last commit, in the order they were made. */
  public List<Change> changes() {
    List<Change> changes = new ArrayList<>();
    staged.forEach((key, value) -> changes.add(new Change(key, value.orElse(null))));
    return changes;
  }

  /** Applies the staged changes to the committed state. */
  public void commit() {
    staged.forEach(
        (key, value) -> {
          if (value.isPresent()) committed.put(key, value.get());
          else committed.remove(key);
        });
    staged.clear();
  }

  /** Forgets the staged changes. */
  public void discard() {
    staged.clear();
  }
}
