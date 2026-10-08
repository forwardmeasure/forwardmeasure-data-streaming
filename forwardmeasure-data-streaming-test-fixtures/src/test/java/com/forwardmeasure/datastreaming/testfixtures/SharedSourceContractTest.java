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
package com.forwardmeasure.datastreaming.testfixtures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** Provider source vocabulary has one owner across FDS transport and FEI destination contracts. */
class SharedSourceContractTest {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

  @Test
  void customerMasterFixtureUsesTheSharedSourceVocabulary() throws Exception {
    var contract = shared("contracts/state-street-customer-master-source-v1.yaml");
    var fields =
        headerFields(
            TestCustomerMasterFixtures.SAMPLE_CSV,
            contract.path("source").path("delimiter").asText());
    var declared = new LinkedHashSet<String>();
    contract.path("source_schema").path("properties").fieldNames().forEachRemaining(declared::add);
    assertEquals(fields, declared, "Source columns must not drift between FDS and FEI");
    assertEquals("UTF-8", contract.path("source").path("encoding").asText());
    assertEquals(
        "ENTITYS_UNIQUE_ID", contract.path("identity").path("source_fields").get(0).asText());
  }

  @Test
  void worldCheckTransportFixtureColumnsBelongToTheSharedSourceContract() throws Exception {
    var contract = shared("contracts/worldcheck-source-v1.json");
    var fields =
        headerFields(
            WorldCheckFixtures.SAMPLE_TSV, contract.path("source").path("delimiter").asText());
    for (String field : fields)
      assertTrue(
          contract.path("source_schema").path("properties").has(field),
          "Undeclared transport fixture column: " + field);
    assertEquals(
        WorldCheckFixtures.ISO_8859_1.name(), contract.path("source").path("encoding").asText());
    assertEquals("UID", contract.path("identity").path("source_fields").get(0).asText());
  }

  private JsonNode shared(String name) throws Exception {
    var copies = Collections.list(getClass().getClassLoader().getResources(name));
    assertEquals(1, copies.size(), "Shared contract must not be shadowed: " + name);
    try (var input = getClass().getResourceAsStream("/" + name)) {
      assertNotNull(input, name);
      return YAML.readTree(input);
    }
  }

  private static LinkedHashSet<String> headerFields(String csv, String delimiter) {
    var names = new LinkedHashSet<String>();
    Arrays.stream(csv.lines().findFirst().orElseThrow().split(Pattern.quote(delimiter), -1))
        .filter(name -> !name.isBlank())
        .forEach(names::add);
    return names;
  }
}
