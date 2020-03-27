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
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;

public class CompactionServiceImpl implements CompactionService {
  private final CompactionPlanner planner = null;
  private final Map<String,CompactionExecutor> executors;
  //TODO configurable
  private final Id myId = Id.of("default");
  private Map<KeyExtent,List<SubmittedJob>> submittedJobs;

  public CompactionServiceImpl() {
    this.executors = Map.of("small", new CompactionExecutor("small", 3), "medium",
        new CompactionExecutor("medium", 3), "large", new CompactionExecutor("large", 3), "huge",
        new CompactionExecutor("huge", 2));
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
  public void compact(CompactionType type, Compactable compactable) {
    // TODO may need to sync
    var files = compactable.getFiles(myId, type);
    if (files.isPresent()) {

      var plan = planner.makePlan(type, files.get(), compactable.getCompactionRatio());
      Collection<CompactionJob> jobs = plan.getJobs();
      List<SubmittedJob> submitted = submittedJobs.getOrDefault(compactable.getExtent(), List.of());

      if (reconcile(jobs, submitted)) {
        for (CompactionJob job : jobs) {
          var sjob = executors.get(job.getExecutor()).submit(myId, job, compactable);
          submittedJobs.computeIfAbsent(compactable.getExtent(), k -> new ArrayList<>()).add(sjob);
        }
      }
    }

  }

}
