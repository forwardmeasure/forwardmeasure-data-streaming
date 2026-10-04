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
package com.forwardmeasure.datastreaming.launcher.jaxrs.mapper;

import com.forwardmeasure.platform.server.jaxrs.UnhandledExceptionProblemMapper;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.regex.Pattern;

/**
 * Translates {@link NullPointerException} into 400 Bad Request - specifically the ones {@code
 * DirectLaunchRequest}/{@code WorkflowLaunchRequest}'s own compact constructors raise via {@code
 * Objects.requireNonNull} when a request body this module's resources deserialize directly off the
 * wire is missing a required field. Those request records are this API's only request bodies and
 * their constructors are this API's only real validation mechanism, so an {@link
 * NullPointerException} reaching this layer means exactly one thing: a missing required field,
 * never an internal bug - unlike a blanket app-wide NPE-to-400 mapping, which would be dangerous
 * precisely because it could mask a real defect as a client error.
 *
 * <p>Not {@code final} - see {@code SecurityExceptionMapper}'s own javadoc for why.
 */
@Provider
public class NullPointerExceptionMapper implements ExceptionMapper<NullPointerException> {
  private static final Pattern ARGUMENT_NAME = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$]*");

  @Override
  public Response toResponse(NullPointerException exception) {
    // A request record refusing a null argument names it: Objects.requireNonNull(spec, "spec").
    // Any other NullPointerException is a server bug - the shared 500 problem, not a 400.
    String argument = exception.getMessage();
    if (argument != null && ARGUMENT_NAME.matcher(argument).matches()) {
      return LauncherProblems.required(argument);
    }
    return new UnhandledExceptionProblemMapper().toResponse(exception);
  }
}
