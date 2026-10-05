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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.forwardmeasure.datastreaming.api.MergePolicy;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CorrelationMergeTest {
  @Test
  void keepsTrustWinnerAndRetainsNamesAndIdentifiersFromThreeSources() {
    Map<String, Object> high =
        Map.of(
            "status",
            "active",
            "names",
            List.of(name("A", "PRIMARY")),
            "identifiers",
            List.of("one", "shared"));
    Map<String, Object> medium =
        Map.of(
            "status",
            "inactive",
            "names",
            List.of(name("B", "PRIMARY")),
            "identifiers",
            List.of("two", "shared"));
    Map<String, Object> low =
        Map.of(
            "names",
            List.of(name("C", "PRIMARY"), name("D", "ALIAS")),
            "identifiers",
            List.of("three"));
    var result = CorrelationMerge.merge(List.of(high, medium, low), MergePolicy.defaults());
    assertEquals("active", result.get("status"));
    assertEquals(
        List.of(name("A", "PRIMARY"), name("B", "ALIAS"), name("C", "ALIAS"), name("D", "ALIAS")),
        result.get("names"));
    assertEquals(List.of("one", "shared", "two", "three"), result.get("identifiers"));
    assertEquals(List.of(name("B", "PRIMARY")), medium.get("names"));
    // Kafka's ordered outer joins must agree with bounded/Spark's whole-group fold.
    assertEquals(
        result,
        CorrelationMerge.merge(
            List.of(CorrelationMerge.merge(List.of(high, medium), MergePolicy.defaults()), low),
            MergePolicy.defaults()));
  }

  @Test
  void replacingOneSourceRemovesItsOldAliasesAndIdentifiers() {
    var high = Map.<String, Object>of("names", List.of(name("A", "PRIMARY")));
    var previous =
        Map.<String, Object>of(
            "names", List.of(name("B", "PRIMARY")), "identifiers", List.of("old"));
    var latest =
        Map.<String, Object>of(
            "names", List.of(name("C", "PRIMARY")), "identifiers", List.of("new"));
    var before = CorrelationMerge.merge(List.of(high, previous), MergePolicy.defaults());
    var after = CorrelationMerge.merge(List.of(high, latest), MergePolicy.defaults());
    assertEquals(List.of("old"), before.get("identifiers"));
    assertEquals(List.of("new"), after.get("identifiers"));
    assertEquals(List.of(name("A", "PRIMARY"), name("C", "ALIAS")), after.get("names"));
  }

  @Test
  void policyControlsFieldNamesWithoutDomainBranchesInTheMerge() {
    var policy =
        new MergePolicy(
            1,
            Map.of(
                "externalIds",
                new MergePolicy.FieldRule(MergePolicy.Strategy.UNION, null, null, null)));
    assertEquals(
        List.of("a", "b"),
        CorrelationMerge.merge(
                List.of(Map.of("externalIds", List.of("a")), Map.of("externalIds", List.of("b"))),
                policy)
            .get("externalIds"));
    assertThrows(IllegalArgumentException.class, () -> new MergePolicy(2, Map.of()));
  }

  private static Map<String, String> name(String name, String type) {
    return Map.of("name", name, "name_type", type);
  }
}
