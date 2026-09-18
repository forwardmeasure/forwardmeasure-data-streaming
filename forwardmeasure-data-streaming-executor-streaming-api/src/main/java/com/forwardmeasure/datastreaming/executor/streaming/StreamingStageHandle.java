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
package com.forwardmeasure.datastreaming.executor.streaming;

/**
 * One running continuous stage, started from one {@code StreamingStageSpec}. Closing it stops the
 * stage - deliberately {@link AutoCloseable}, not a fire-and-forget submit like fowf's own {@code
 * ExecutionEngineProvider#submit}: a streaming stage has no terminal state to wait for (see this
 * repo's own continuous-streaming gap-bridging plan, "continuous execution" gap row), so lifecycle
 * here is start/observe/stop, not submit/await-completion.
 */
public interface StreamingStageHandle extends AutoCloseable {
  String stageName();

  StreamingStageHealth health();

  @Override
  void close();
}
