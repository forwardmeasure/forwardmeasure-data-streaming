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
package com.forwardmeasure.datastreaming.core;

import com.forwardmeasure.datastreaming.api.MergePolicy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Engine-neutral merge of contributions ordered by descending trust, then stable source key. */
public final class CorrelationMerge {
  private CorrelationMerge() {}

  public static Map<String, Object> merge(
      List<Map<String, Object>> contributions, MergePolicy policy) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Map<String, Object> contribution : contributions) {
      if (contribution == null) continue;
      for (var field : contribution.entrySet()) {
        Object value = field.getValue();
        if (value == null) continue;
        MergePolicy.FieldRule rule = policy.fields().get(field.getKey());
        if (rule == null || rule.strategy() == MergePolicy.Strategy.TRUST) {
          result.putIfAbsent(field.getKey(), value);
          continue;
        }
        if (rule.strategy() == MergePolicy.Strategy.OBJECTS) {
          if (!(value instanceof Map<?, ?> incoming))
            throw new IllegalArgumentException("OBJECTS merge requires an object");
          Map<Object, Object> object = new LinkedHashMap<>();
          if (result.get(field.getKey()) instanceof Map<?, ?> prior) object.putAll(prior);
          for (var entry : incoming.entrySet()) {
            List<Object> values = items(object.get(entry.getKey()));
            values.addAll(items(entry.getValue()));
            object.put(entry.getKey(), new ArrayList<>(new LinkedHashSet<>(values)));
          }
          result.put(field.getKey(), object);
          continue;
        }
        List<Object> accumulated = items(result.get(field.getKey()));
        List<Object> incoming = items(value);
        if (rule.strategy() == MergePolicy.Strategy.ALIASES) {
          boolean hasPrimary = accumulated.stream().anyMatch(item -> isPrimary(item, rule));
          for (Object item : incoming) {
            if (isPrimary(item, rule)) {
              if (hasPrimary) {
                // A corroborating primary is not an extra alias of the same name.
                if (accumulated.contains(item)) continue;
                Map<Object, Object> alias = new LinkedHashMap<>((Map<?, ?>) item);
                alias.put(rule.discriminator(), rule.alias());
                item = alias;
              } else {
                hasPrimary = true;
              }
            }
            accumulated.add(item);
          }
        } else {
          accumulated.addAll(incoming);
        }
        result.put(field.getKey(), new ArrayList<>(new LinkedHashSet<>(accumulated)));
      }
    }
    return result;
  }

  private static boolean isPrimary(Object item, MergePolicy.FieldRule rule) {
    return item instanceof Map<?, ?> map && rule.primary().equals(map.get(rule.discriminator()));
  }

  private static List<Object> items(Object value) {
    if (value == null) return new ArrayList<>();
    return value instanceof Collection<?> collection
        ? new ArrayList<>(collection)
        : new ArrayList<>(List.of(value));
  }
}
