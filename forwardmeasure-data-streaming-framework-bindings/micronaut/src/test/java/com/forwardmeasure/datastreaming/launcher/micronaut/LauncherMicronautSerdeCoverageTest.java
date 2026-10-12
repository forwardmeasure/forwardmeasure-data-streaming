/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.forwardmeasure.datastreaming.launcher.micronaut;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.core.beans.BeanIntrospector;
import io.micronaut.serde.annotation.SerdeImport;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Micronaut Serde encodes only types it has compile-time introspection for, and this module's
 * request/response types come from other jars, so {@link LauncherMicronautBinding} lists each one
 * by hand. A type nested inside a listed one (for example {@code WorkflowExecution}'s effects and
 * timers) is not covered transitively: a missing entry compiles, starts, and fails only when that
 * body is first encoded at runtime. This walks every project type reachable from the declared
 * {@code @SerdeImport} roots and requires each to be imported and introspected.
 */
class LauncherMicronautSerdeCoverageTest {
  private static final String PROJECT_PACKAGE = "com.forwardmeasure.";

  @Test
  void everyReachableProjectTypeIsSerdeImportedAndIntrospected() {
    Set<Class<?>> imported =
        Arrays.stream(LauncherMicronautBinding.class.getAnnotationsByType(SerdeImport.class))
            .map(SerdeImport::value)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    assertTrue(!imported.isEmpty(), "LauncherMicronautBinding declares no @SerdeImport");

    Set<Class<?>> reachable = reachableProjectTypes(imported);

    Set<String> notImported = new TreeSet<>();
    Set<String> notIntrospected = new TreeSet<>();
    for (Class<?> type : reachable) {
      if (!imported.contains(type)) notImported.add(type.getName());
      if (!type.isEnum() && BeanIntrospector.SHARED.findIntrospection(type).isEmpty()) {
        notIntrospected.add(type.getName());
      }
    }
    assertTrue(
        notImported.isEmpty(),
        "Reachable from the launcher's JSON bodies but missing @SerdeImport on"
            + " LauncherMicronautBinding: "
            + notImported);
    assertTrue(
        notIntrospected.isEmpty(),
        "Reachable from the launcher's JSON bodies but missing from"
            + " LauncherMicronautBinding's @Introspected(classes = ...): "
            + notIntrospected);
  }

  private static Set<Class<?>> reachableProjectTypes(Set<Class<?>> roots) {
    Set<Class<?>> seen = new LinkedHashSet<>();
    Deque<Type> pending = new ArrayDeque<>(roots);
    while (!pending.isEmpty()) {
      Type next = pending.pop();
      switch (next) {
        case Class<?> type when type.isArray() -> pending.push(type.getComponentType());
        case Class<?> type -> {
          if (!type.getName().startsWith(PROJECT_PACKAGE) || !seen.add(type)) continue;
          pending.addAll(propertyTypes(type));
        }
        case ParameterizedType parameterized -> {
          pending.push(parameterized.getRawType());
          pending.addAll(List.of(parameterized.getActualTypeArguments()));
        }
        case GenericArrayType array -> pending.push(array.getGenericComponentType());
        case WildcardType wildcard -> pending.addAll(List.of(wildcard.getUpperBounds()));
        case TypeVariable<?> variable -> pending.addAll(List.of(variable.getBounds()));
        default -> {}
      }
    }
    return seen;
  }

  private static List<Type> propertyTypes(Class<?> type) {
    if (type.isEnum()) return List.of();
    if (type.isRecord()) {
      return Arrays.stream(type.getRecordComponents())
          .map(RecordComponent::getGenericType)
          .toList();
    }
    List<Type> types = new java.util.ArrayList<>();
    for (Class<?> current = type;
        current != null && current.getName().startsWith(PROJECT_PACKAGE);
        current = current.getSuperclass()) {
      for (Field field : current.getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) continue;
        types.add(field.getGenericType());
      }
    }
    return types;
  }
}
