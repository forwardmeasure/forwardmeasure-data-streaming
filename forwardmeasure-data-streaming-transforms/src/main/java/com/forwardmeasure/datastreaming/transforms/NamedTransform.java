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
package com.forwardmeasure.datastreaming.transforms;

import com.forwardmeasure.datastreaming.api.TransformCharacteristics;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * A named row function a {@code TransformSpec} field rule refers to. It declares its own {@link
 * TransformCharacteristics}, so the planner learns what it needs from the transform itself rather
 * than from a separate table. The default is {@link TransformCharacteristics#PURE_ROW_FUNCTION}.
 */
@FunctionalInterface
public interface NamedTransform extends Serializable {
  Object apply(Map<String, String> inputs);

  default TransformCharacteristics characteristics() {
    return TransformCharacteristics.PURE_ROW_FUNCTION;
  }

  /** A transform with characteristics other than the pure-row-function default. */
  static NamedTransform of(TransformCharacteristics characteristics, NamedTransform function) {
    Objects.requireNonNull(characteristics, "characteristics");
    Objects.requireNonNull(function, "function");
    return new Declared(characteristics, function);
  }

  /** {@link #of}'s implementation. */
  record Declared(TransformCharacteristics characteristics, NamedTransform function)
      implements NamedTransform {
    @Override
    public Object apply(Map<String, String> inputs) {
      return function.apply(inputs);
    }
  }
}
