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
package com.forwardmeasure.datastreaming.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.datastreaming.transforms.NamedTransformRegistry;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link TransformCharacteristicsRegistry} cannot silently drift apart from {@link
 * NamedTransformRegistry} - every real, callable named transform must have characteristics the
 * planner can read, and every declared characteristics entry must name a real, callable transform.
 */
class TransformCharacteristicsRegistryTest {

  @Test
  void everyNamedTransformHasCharacteristics() {
    for (String name : NamedTransformRegistry.names()) {
      assertTrue(
          TransformCharacteristicsRegistry.contains(name),
          "NamedTransformRegistry has '"
              + name
              + "' but TransformCharacteristicsRegistry does not - the planner cannot resolve an"
              + " engine for a spec using this transform");
    }
  }

  @Test
  void everyCharacteristicsEntryNamesARealTransform() {
    for (String name : TransformCharacteristicsRegistry.names()) {
      assertTrue(
          NamedTransformRegistry.contains(name),
          "TransformCharacteristicsRegistry declares '"
              + name
              + "' but NamedTransformRegistry has no such transform - this entry can never be"
              + " reached by a real FieldRule");
    }
  }

  @Test
  void theTwoRegistriesShareExactlyTheSameNames() {
    assertEquals(NamedTransformRegistry.names(), TransformCharacteristicsRegistry.names());
  }

  @Test
  void unknownNameThrows() {
    assertThrows(
        IllegalArgumentException.class, () -> TransformCharacteristicsRegistry.get("nope"));
  }
}
