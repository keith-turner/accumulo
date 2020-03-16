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

import java.util.Map;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.Cancellation;
import org.apache.accumulo.core.spi.compaction.CompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SubmittedJob;
import org.apache.accumulo.core.spi.compaction.SubmittedJob.Status;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private CompactionPlanner planner;
  private Iterable<Compactable> compactables;
  private Map<String,CompactionExecutor> executors;
  private Table<KeyExtent,CompactionId,SubmittedJob> submittedJobs;
  private long nextId = 0;

  private void mainLoop() {
    while (true) {
      try {
        for (Compactable compactable : compactables) {

          // TODO look to avoid linear search
          submittedJobs.cellSet().removeIf(cell -> {
            var status = cell.getValue().getStatus();
            return status == Status.COMPLETE || status == Status.FAILED;
          });

          CompactionPlan plan = planner.makePlan(new PlanningParametersImpl(compactable,
              submittedJobs.row(compactable.getExtent()).values()));

          log.info("Compaction plan for " + compactable.getExtent() + " " + plan);

          for (Cancellation cancelation : plan.getCancellations()) {
            SubmittedJob sjob = Iterables
                .getOnlyElement(submittedJobs.column(cancelation.getCompactionId()).values());
            executors.get(sjob.getJob().getExecutor()).cancel(cancelation);
            // TODO check if canceled
            // TODO may be more efficient to cancel batches
          }

          for (CompactionJob job : plan.getJobs()) {
            CompactionId compId = CompactionId.of(nextId++);
            SubmittedJob sjob = executors.get(job.getExecutor()).submit(job, compId, compactable);
            submittedJobs.put(compactable.getExtent(), compId, sjob);
          }
        }
      } catch (Exception e) {
        // TODO
        log.error("Loop failed ", e);
      }
      UtilWaitThread.sleep(3000);// TODO
    }
  }

  public CompactionManager(Iterable<Compactable> compactables) {
    this.compactables = compactables;
    // TODO confiugrable
    this.planner = new TieredCompactionManager(2);
    this.executors = Map.of("small", new CompactionExecutor(3), "medium", new CompactionExecutor(3),
        "large", new CompactionExecutor(3), "huge", new CompactionExecutor(2));
    this.submittedJobs = HashBasedTable.create();

  }

  public void start() {
    // TODO deamon thread
    // TODO stop method
    log.info("Started compaction manager");
    new Thread(() -> mainLoop()).start();
  }
}
