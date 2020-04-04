/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.admin.compaction;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;

/**
 * Enables dynamically overriding of configuration used to create the output file for a compaction.
 *
 * @since 2.1.0
 */
public interface CompactionConfigurer {
  /**
   * @since 2.1.0
   */
  public interface InitParamaters {
    Map<String,String> getOptions();

    PluginEnvironment getEnvironment();
  }

  void init(InitParamaters iparams);

  /**
   * @since 2.1.0
   */
  public interface InputParameters {
    public Collection<TabletFileInfo> getInputFiles();

    PluginEnvironment getEnvironment();
  }

  /**
   * Specifies how the output file should be created for a compaction.
   *
   * @since 2.1.0
   */
  public class Overrides {
    // TODO builder
    public Map<String,String> tabletPropertyOverrides;
  }

  Overrides override(InputParameters params);
}
