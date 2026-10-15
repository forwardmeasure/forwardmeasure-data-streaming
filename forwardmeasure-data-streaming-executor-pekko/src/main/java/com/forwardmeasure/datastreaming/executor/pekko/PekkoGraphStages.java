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
package com.forwardmeasure.datastreaming.executor.pekko;

import com.forwardmeasure.datastreaming.api.IngestionSpec;
import com.forwardmeasure.datastreaming.core.IngestionPipeline.MalformedRecordPolicy;
import com.forwardmeasure.datastreaming.core.stateful.GraphRecords;
import com.forwardmeasure.datastreaming.core.stateful.StatefulGraph;
import java.util.Map;
import org.apache.pekko.NotUsed;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Source;

/**
 * The Pekko stream stages that run a spec's {@link
 * com.forwardmeasure.datastreaming.api.TransformGraph}.
 */
final class PekkoGraphStages {

  private PekkoGraphStages() {}

  /**
   * A bounded run's graph stage: each mapped record through the graph in order, then - once the
   * input completes - every window still open.
   */
  static Flow<Map<String, Object>, Map<String, Object>, NotUsed> bounded(
      IngestionSpec spec, String sourceKey) {
    MalformedRecordPolicy policy = MalformedRecordPolicy.from(spec.errors());
    return Flow.lazyFlow(
            () -> {
              // One graph, and so one set of state, per materialization: per run.
              StatefulGraph graph = GraphRecords.bounded(spec.transforms(), spec.blockingField());
              return Flow.<Map<String, Object>>create()
                  .mapConcat(record -> GraphRecords.process(graph, sourceKey, record, policy))
                  .concatLazy(
                      Source.lazySource(() -> Source.from(GraphRecords.finish(graph, policy))));
            })
        .mapMaterializedValue(ignored -> NotUsed.getInstance());
  }
}
