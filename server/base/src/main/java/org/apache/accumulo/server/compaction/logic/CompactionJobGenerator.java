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
package org.apache.accumulo.server.compaction.logic;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;

public class CompactionJobGenerator {

  private final CompactionServicesConfig servicesConfig;
  private final Map<CompactionServiceId,CompactionPlanner> planners = new HashMap<>();
  private final Map<TableId,CompactionDispatcher> dispatchers = new HashMap<>();
  private final Set<CompactionServiceId> serviceIds;

  CompactionJobGenerator(ServiceEnvironment env) {
    servicesConfig = new CompactionServicesConfig(env.getConfiguration());
    serviceIds = servicesConfig.getPlanners().keySet().stream().map(CompactionServiceId::of)
        .collect(Collectors.toUnmodifiableSet());
  }

  Map<CompactionServiceId,List<CompactionJob>> generateJobs(ServiceEnvironment env,
      CompactionKind kind, TabletMetadata tablet) {

    CompactionServiceId serviceId = dispatch(env, kind, tablet);

    Collection<CompactionJob> jobs = planCompactions(env, serviceId, kind, tablet);

    return null;
  }

  private CompactionServiceId dispatch(ServiceEnvironment env, CompactionKind kind,
      TabletMetadata tablet) {
    CompactionDispatcher dispatcher =
        dispatchers.computeIfAbsent(tablet.getTableId(), tableId -> createDispatcher(env, tableId));

    CompactionDispatcher.DispatchParameters dispatchParams =
        new CompactionDispatcher.DispatchParameters() {
          @Override
          public CompactionServices getCompactionServices() {
            return () -> serviceIds;
          }

          @Override
          public ServiceEnvironment getServiceEnv() {
            return env;
          }

          @Override
          public CompactionKind getCompactionKind() {
            return kind;
          }

          @Override
          public Map<String,String> getExecutionHints() {
            // TODO do for user compactions
            return Map.of();
          }
        };

    return dispatcher.dispatch(dispatchParams).getService();
  }

  private CompactionDispatcher createDispatcher(ServiceEnvironment env, TableId tableId) {

    var conf = env.getConfiguration();

    var className = conf.get(Property.TABLE_COMPACTION_DISPATCHER.getKey());

    Map<String,String> opts = new HashMap<>();

    conf.getWithPrefix(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey()).forEach((k, v) -> {
      opts.put(k.substring(Property.TABLE_COMPACTION_DISPATCHER.getKey().length()), v);
    });

    var finalOpts = Collections.unmodifiableMap(opts);

    CompactionDispatcher.InitParameters initParameters = new CompactionDispatcher.InitParameters() {
      @Override
      public Map<String,String> getOptions() {
        return finalOpts;
      }

      @Override
      public TableId getTableId() {
        return tableId;
      }

      @Override
      public ServiceEnvironment getServiceEnv() {
        return env;
      }
    };

    CompactionDispatcher dispatcher = null;
    try {
      dispatcher = env.instantiate(className, CompactionDispatcher.class);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }

    dispatcher.init(initParameters);

    return dispatcher;
  }

  private Collection<CompactionJob> planCompactions(ServiceEnvironment env,
      CompactionServiceId serviceId, CompactionKind kind, TabletMetadata tablet) {

    CompactionPlanner planner =
        planners.computeIfAbsent(serviceId, sid -> createPlanner(env, serviceId));

    // selecting indicator
    // selected files

    String ratioStr =
        env.getConfiguration(tablet.getTableId()).get(Property.TABLE_MAJC_RATIO.getKey());
    if (ratioStr == null) {
      ratioStr = Property.TABLE_MAJC_RATIO.getDefaultValue();
    }

    double ratio = Double.parseDouble(ratioStr);

    Set<CompactableFile> allFiles = tablet.getFilesMap().entrySet().stream()
        .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
        .collect(Collectors.toUnmodifiableSet());
    Set<CompactableFile> candidates;

    if (kind == CompactionKind.SYSTEM) {
      if (tablet.getExternalCompactions().isEmpty()) {
        candidates = allFiles;
      } else {
        var tmpFiles = new HashMap<>(tablet.getFilesMap());
        tablet.getExternalCompactions().values().stream().flatMap(ecm -> ecm.getJobFiles().stream())
            .forEach(tmpFiles::remove);
        candidates = tmpFiles.entrySet().stream()
            .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
            .collect(Collectors.toUnmodifiableSet());
      }
    } else {
      throw new UnsupportedOperationException();
    }

    CompactionPlanner.PlanningParameters params = new CompactionPlanner.PlanningParameters() {
      @Override
      public TableId getTableId() {
        return tablet.getTableId();
      }

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return env;
      }

      @Override
      public CompactionKind getKind() {
        return kind;
      }

      @Override
      public double getRatio() {
        return ratio;
      }

      @Override
      public Collection<CompactableFile> getAll() {
        return allFiles;
      }

      @Override
      public Collection<CompactableFile> getCandidates() {
        return candidates;
      }

      @Override
      public Collection<CompactionJob> getRunningCompactions() {
        var allFiles2 = tablet.getFilesMap();
        return tablet.getExternalCompactions().values().stream().map(ecMeta -> {
          Collection<CompactableFile> files = ecMeta.getJobFiles().stream()
              .map(f -> new CompactableFileImpl(f, allFiles2.get(f))).collect(Collectors.toList());
          CompactionJob job = new CompactionJobImpl(ecMeta.getPriority(),
              ecMeta.getCompactionExecutorId(), files, ecMeta.getKind(), Optional.empty());
          return job;
        }).collect(Collectors.toList());
      }

      @Override
      public Map<String,String> getExecutionHints() {
        // TODO implement for user compactions
        return Map.of();
      }

      @Override
      public CompactionPlan.Builder createPlanBuilder() {
        return new CompactionPlanImpl.BuilderImpl(kind, allFiles, candidates);
      }
    };

    return planner.makePlan(params).getJobs();
  }

  private CompactionPlanner createPlanner(ServiceEnvironment env, CompactionServiceId serviceId) {

    String plannerClassName = servicesConfig.getPlanners().get(serviceId.canonical());

    CompactionPlanner planner = null;
    try {
      planner = env.instantiate(plannerClassName, CompactionPlanner.class);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }

    CompactionPlanner.InitParameters initParameters = new CompactionPlanner.InitParameters() {
      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return env;
      }

      @Override
      public Map<String,String> getOptions() {
        return servicesConfig.getPlanners();
      }

      @Override
      public String getFullyQualifiedOption(String key) {
        return Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + serviceId + ".opts." + key;
      }

      @Override
      public ExecutorManager getExecutorManager() {
        return new ExecutorManager() {
          @Override
          public CompactionExecutorId createExecutor(String name, int threads) {
            // TODO need to deprecate
            throw new UnsupportedOperationException();
          }

          @Override
          public CompactionExecutorId getExternalExecutor(String name) {
            return CompactionExecutorIdImpl.externalId(name);
          }
        };
      }
    };

    planner.init(initParameters);

    return planner;
  }
}
