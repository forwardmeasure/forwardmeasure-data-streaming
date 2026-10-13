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

import java.io.IOException;
import java.io.InputStream;

/**
 * Source text the caller opens itself, for {@link PekkoIngestionRunner#run(
 * com.forwardmeasure.datastreaming.api.IngestionSpec, TextContent,
 * org.apache.pekko.actor.ActorSystem, java.util.function.Function,
 * org.apache.pekko.stream.javadsl.Sink)} - for example one pinned version of an object-storage
 * object, read in a single pass without a staging copy. Opened once, on the stream's blocking-IO
 * dispatcher, and closed when the source ends, fails or is cancelled.
 */
@FunctionalInterface
public interface TextContent {
  InputStream open() throws IOException;
}
