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
package org.apache.accumulo.tserver.compactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;

import com.google.gson.Gson;

public class CompactionServiceImpl implements CompactionService {
  private final CompactionPlanner planner;
  private final Map<String,CompactionExecutor> executors;
  // TODO configurable
  private final Id myId = Id.of("default");
  private Map<KeyExtent,List<SubmittedJob>> submittedJobs = new ConcurrentHashMap<>();

  static class ExecutorConfig {
    String name;
    String maxSize;
    int numThreads;
  }

  static class ServiceConfig {
    List<ExecutorConfig> executors;
  }

  public CompactionServiceImpl(String config) {
    Gson gson = new Gson();
    var serviceConfig = gson.fromJson(config, ServiceConfig.class);

    Map<String,CompactionExecutor> tmpExecutors = new HashMap<>();
    for (ExecutorConfig execConfig : serviceConfig.executors) {
      tmpExecutors.put(execConfig.name,
          new CompactionExecutor(execConfig.name, execConfig.numThreads));
    }

    this.executors = Map.copyOf(tmpExecutors);

    this.planner = new TieredCompactionPlanner(serviceConfig);
  }

  private boolean reconcile(Collection<CompactionJob> jobs, List<SubmittedJob> submitted) {
    for (SubmittedJob submittedJob : submitted) {
      if (submittedJob.getStatus() == Status.QUEUED) {
        // TODO this is O(M*N) unless set
        if (!jobs.remove(submittedJob.getJob())) {
          if (!submittedJob.cancel(Status.QUEUED)) {
            return false;
          }
        }
      } else if (submittedJob.getStatus() == Status.RUNNING) {
        for (CompactionJob job : jobs) {
          if (!Collections.disjoint(submittedJob.getJob().getFiles(), job.getFiles())) {
            return false;
          }
        }
      } else {
        // TODO remove from submitted jobs
      }
    }

    return true;
  }

  @Override
  public void compact(CompactionType type, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    // TODO this could take a while... could run this in a thread pool
    var files = compactable.getFiles(myId, type);
    if (files.isPresent()) {

      var plan = planner.makePlan(type, files.get(), compactable.getCompactionRatio());
      Set<CompactionJob> jobs = new HashSet<>(plan.getJobs());
      List<SubmittedJob> submitted = submittedJobs.getOrDefault(compactable.getExtent(), List.of());

      if (reconcile(jobs, submitted)) {
        for (CompactionJob job : jobs) {
          var sjob =
              executors.get(job.getExecutor()).submit(myId, job, compactable, completionCallback);
          submittedJobs.computeIfAbsent(compactable.getExtent(), k -> new ArrayList<>()).add(sjob);
        }
      }
    }
  }
}
