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
package org.apache.accumulo.server.conf;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class CheckCompactionConfig implements KeywordExecutable {

  private final static Logger log = LoggerFactory.getLogger(CheckCompactionConfig.class);

  static class Opts extends Help {
    @Parameter(description = "<path/to/props/file>")
    String filePath;
  }

  @Override
  public String keyword() {
    return "check-compaction-config";
  }

  @Override
  public String description() {
    return "Checks compaction config";
  }

  public static void main(String[] args) throws Exception {
    new CheckCompactionConfig().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("accumulo check-compaction-config", args);

    if (opts.filePath == null) {
      throw new IllegalArgumentException("No properties file was given");
    }

    Path path = Path.of(opts.filePath);
    if (!path.toFile().exists())
      throw new FileNotFoundException("File at given path could not be found");

    AccumuloConfiguration config = null; // TODO convert file from command line to Accumulo config
    var servicesConfig = new CompactionServicesConfig(config, msg -> log.warn(msg));

    for (String serviceId : servicesConfig.getPlanners().keySet()) {
      String plannerClass = servicesConfig.getPlanners().get(serviceId);

      // TODO better more standard Accumulo way to load class?
      var planner = getClass().getClassLoader().loadClass(plannerClass)
          .asSubclass(CompactionPlanner.class).newInstance();

      ServiceEnvironment senv = new ServiceEnvironment() {

        @Override
        public <T> T instantiate(TableId tableId, String className, Class<T> base)
            throws Exception {
          throw new UnsupportedOperationException();
        }

        @Override
        public <T> T instantiate(String className, Class<T> base) throws Exception {
          throw new UnsupportedOperationException();
        }

        @Override
        public String getTableName(TableId tableId) throws TableNotFoundException {
          throw new UnsupportedOperationException();
        }

        @Override
        public Configuration getConfiguration(TableId tableId) {
          return new ConfigurationImpl(config);
        }

        @Override
        public Configuration getConfiguration() {
          return new ConfigurationImpl(config);
        }
      };
      var initParams = new CompactionPlannerInitParams(CompactionServiceId.of(serviceId),
          servicesConfig.getOptions().get(serviceId), senv);

      planner.init(initParams);

      initParams.getRequestedExecutors()
          .forEach((execId, numThreads) -> log.info(
              "Compaction service {} requested creation of thread pool {} with {} threads.",
              serviceId, execId, numThreads));

      initParams.getRequestedExternalExecutors()
          .forEach(execId -> log.info(
              "Compaction service {} requested used of external execution queue {}", serviceId,
              execId));

    }

    log.info("Properties file has passed all checks.");
  }
}
