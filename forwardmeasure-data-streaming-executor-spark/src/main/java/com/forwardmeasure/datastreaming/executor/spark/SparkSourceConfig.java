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
package com.forwardmeasure.datastreaming.executor.spark;

import com.forwardmeasure.datastreaming.api.SourceSpec;
import com.forwardmeasure.datastreaming.api.TransformSpec;
import java.io.Serializable;

/**
 * One source to correlate: a real FDS {@link SourceSpec} (connector/uri/format/query) + {@link
 * TransformSpec} (D3, corrected - the runtime-interpreted field mapping, not a compiled schema),
 * plus this source's {@code trustWeight} for the merge step. Generalizes fei's own {@code
 * SourceConfig} - the same plain, {@link Serializable} shape (real Spark closures ship this to
 * every executor; {@code SourceSpec}/{@code TransformSpec} are safely serializable themselves, D3),
 * driven by this library's own generated spec types instead of a source-specific YAML record.
 */
public record SparkSourceConfig(
    String sourceKey, SourceSpec source, TransformSpec mapper, double trustWeight)
    implements Serializable {

  /** Defaults to 1.0 (all sources trusted equally) when a pipeline doesn't declare trust tiers. */
  public double effectiveTrustWeight() {
    return trustWeight;
  }
}
