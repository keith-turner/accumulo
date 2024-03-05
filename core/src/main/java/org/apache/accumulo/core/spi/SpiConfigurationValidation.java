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
package org.apache.accumulo.core.spi;

import org.apache.accumulo.core.client.PluginEnvironment;

/**
 * Interface to be used on SPI classes where custom properties are in use. When
 * {@code #validateConfiguration(org.apache.accumulo.core.client.PluginEnvironment.Configuration)}
 * is called validation of the custom properties should be performed, if applicable. Implementations
 * should log any configuration issues at the warning level before finally returning false.
 *
 */
public interface SpiConfigurationValidation {

  /**
   * Validates implementation custom property configuration
   *
   * @param conf configuration
   * @return true if configuration is valid, else false after logging all warnings
   */
  public boolean validateConfiguration(PluginEnvironment.Configuration conf);

}
