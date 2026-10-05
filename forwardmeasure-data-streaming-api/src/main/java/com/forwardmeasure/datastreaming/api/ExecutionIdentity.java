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
package com.forwardmeasure.datastreaming.api;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;

/**
 * Stable consumer/changelog identity across restarts; callers can supply their tenant-scoped run
 * key.
 */
public final class ExecutionIdentity {
  private ExecutionIdentity() {}

  public static String of(ExecutionPlan plan, String prefix, String runKey) {
    try {
      var mapper =
          JsonMapper.builder()
              .addModule(new JavaTimeModule())
              .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
              .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
              .build();
      var identity = new LinkedHashMap<String, Object>();
      identity.put("runKey", runKey);
      identity.put("profile", plan.profile());
      identity.put("sources", plan.sources());
      identity.put("transforms", plan.transforms());
      identity.put("delivery", plan.delivery());
      identity.put("errors", plan.errors());
      identity.put("blockingField", plan.blockingField());
      identity.put("destination", plan.destination());
      identity.put("mergePolicy", plan.mergePolicy());
      String canonical = mapper.writeValueAsString(identity);
      return prefix
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(canonical.getBytes(StandardCharsets.UTF_8)),
                  0,
                  16);
    } catch (Exception failure) {
      throw new IllegalArgumentException("Unable to identify execution", failure);
    }
  }
}
