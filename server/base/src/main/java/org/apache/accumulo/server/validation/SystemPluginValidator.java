/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.validation;

import java.util.List;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.validation.system.CompactionPlannersValidator;
import org.apache.accumulo.server.validation.system.SystemBalancerValidator;
import org.apache.accumulo.server.validation.system.SystemCryptoServiceValidator;

/**
 * Implementations of this class know how to validate a particular type of system level Accumulo
 * plugin.
 */
public interface SystemPluginValidator {

  /**
   * Validate a plugin type based on current configuration. If something is not valid,
   * implementations should log any configuration issues at the warning level before finally
   * returning false.
   */
  boolean validate(ServerContext context);

  /**
   * @return a list of all known system level plugin validators
   */
  static List<SystemPluginValidator> validators() {
    return List.of(new CompactionPlannersValidator(), new SystemBalancerValidator(),
        new SystemCryptoServiceValidator());
  }
}
