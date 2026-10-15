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
 * FDS's generic, domain-free named transforms and the {@link
 * com.forwardmeasure.datastreaming.transforms.NamedTransformCatalog} a mapping engine and the
 * planner resolve transform names against. A product registers its own transforms through {@link
 * com.forwardmeasure.datastreaming.transforms.NamedTransformProvider}; product or data-provider
 * vocabulary never lives here.
 */
package com.forwardmeasure.datastreaming.transforms;
