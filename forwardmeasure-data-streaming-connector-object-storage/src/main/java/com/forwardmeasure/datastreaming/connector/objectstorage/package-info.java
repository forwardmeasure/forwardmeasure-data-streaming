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
/**
 * Thin adapter over {@code forwardmeasure-object-storage} - the one deliberate exception to "prefer
 * Camel" in this library, since no Camel S3/GCS component beats a provider-neutral interface this
 * org already built, controls, and has a real external consumer of. Not Camel-backed, so it does
 * not go through {@code camel-reactive-streams}.
 */
package com.forwardmeasure.datastreaming.connector.objectstorage;
