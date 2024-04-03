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
package org.apache.accumulo.server.validation.system;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.validation.SystemPluginValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to validate compaction planner plugins configured at the system level.
 */
public class CompactionPlannersValidator implements SystemPluginValidator {

  private static final Logger log = LoggerFactory.getLogger(CompactionPlannersValidator.class);

  @Override
  public boolean validate(ServerContext context) {
    String lastService = null;
    try {
      CompactionServicesConfig csc =
          new CompactionServicesConfig(context.getConfiguration(), log::warn);
      for (var entry : csc.getPlanners().entrySet()) {
        String serviceId = entry.getKey();
        lastService = serviceId;
        String plannerClassName = entry.getValue();

        Class<? extends CompactionPlanner> plannerClass =
            Class.forName(plannerClassName).asSubclass(CompactionPlanner.class);
        CompactionPlanner planner = plannerClass.getDeclaredConstructor().newInstance();

        ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

        var initParams = new CompactionPlannerInitParams(CompactionServiceId.of(serviceId),
            csc.getOptions().get(serviceId), senv);

        planner.init(initParams);

      }
      return true;
    } catch (Exception e) {
      log.warn("Error validating configuration, error may be related to compaction service : "
          + lastService, e);
      return false;
    }
  }
}
