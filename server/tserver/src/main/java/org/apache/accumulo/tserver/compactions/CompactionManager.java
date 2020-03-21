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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.Cancellation;
import org.apache.accumulo.core.spi.compaction.CompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SubmittedJob;
import org.apache.accumulo.core.spi.compaction.SubmittedJob.Status;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private CompactionPlanner planner;
  private Iterable<Compactable> compactables;
  private Map<String,CompactionExecutorImpl> executors;
  private Table<KeyExtent,CompactionId,SubmittedJob> submittedJobs;
  private long nextId = 0;

  private static class CompactablePlan {

    public final CompactionPlan plan;
    public final Compactable compactable;

    public CompactablePlan(CompactionPlan plan, Compactable compactable) {
      this.plan = plan;
      this.compactable = compactable;
    }

  }

  private static Text getEndrow(Compactable c) {
    return c.getExtent().getEndRow();
  }

  private synchronized void printStats() {
    try {

      Map<String,Integer> epos = Map.of("small", 1, "medium", 3, "large", 5, "huge", 7);

      String[] columns = {"not compacting", "small running", "small queued", "medium running",
          "medium queued", "large running", "large queued", "huge running", "huge queued"};

      while (true) {

        List<Compactable> sortedCompactables = new ArrayList<Compactable>();
        compactables.forEach(sortedCompactables::add);
        sortedCompactables.removeIf(
            c -> c.getExtent().isMeta() || c.getExtent().getTableId().canonical().equals("+rep")
                || c.getExtent().getTableId().canonical().equals("1"));
        Collections.sort(sortedCompactables, Comparator.comparing(CompactionManager::getEndrow,
            Comparator.nullsLast(Comparator.naturalOrder())));

        int[][] data = new int[sortedCompactables.size()][];
        String[] rows = new String[sortedCompactables.size()];

        for (int i = 0; i < sortedCompactables.size(); i++) {
          int r = i;
          var compactable = sortedCompactables.get(r);

          rows[r] = compactable.getExtent().getEndRow() + "";

          data[r] = new int[columns.length];

          submittedJobs.row(compactable.getExtent()).values().forEach(sjob -> {
            var status = sjob.getStatus();

            if (status == Status.QUEUED || status == Status.RUNNING) {
              int pos = epos.get(sjob.getJob().getExecutor());
              if (status == Status.QUEUED)
                pos++;
              data[r][pos] = sjob.getJob().getFiles().size();
            }
          });

          data[r][0] = compactable.getFiles().size();
        }

        if (rows.length > 0) {
          System.out.println("Compaction stats : " + new Date());
          System.out.println(new PrintableTable(columns, rows, data).toString());
        }

        try {
          wait(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      log.error("CMSF", e);
    }
  }

  // TODO remove sync... its a hack for printStats
  private synchronized void mainLoop() {
    while (true) {
      try {
        Collection<CompactablePlan> plans = new ArrayList<>();

        int numCompactables = 0;

        for (Compactable compactable : compactables) {

          var plan = planner.makePlan(new PlanningParametersImpl(compactable,
              submittedJobs.row(compactable.getExtent()).values()));

          if (!plan.getJobs().isEmpty() || !plan.getJobs().isEmpty()) {
            log.info("Compaction plan for {} {}", compactable.getExtent(), plan);
            plans.add(new CompactablePlan(plan, compactable));
          }

          numCompactables++;
        }

        if (submittedJobs.size() > 5 * numCompactables) {
          // TODO could linear search be avoided?
          submittedJobs.cellSet().removeIf(cell -> {
            var status = cell.getValue().getStatus();
            return status == Status.COMPLETE || status == Status.FAILED
                || status == Status.CANCELED;
          });
        }

        Map<String,List<Cancellation>> cancellations = new HashMap<>();

        for (CompactablePlan cplan : plans) {
          for (Cancellation cancellation : cplan.plan.getCancellations()) {
            SubmittedJob sjob = Iterables
                .getOnlyElement(submittedJobs.column(cancellation.getCompactionId()).values());
            cancellations.computeIfAbsent(sjob.getJob().getExecutor(), k -> new ArrayList<>())
                .add(cancellation);
          }
        }

        // TODO for cancels that did not succeed do not want to submit job
        Set<CompactionId> succesfullyCancelled = new HashSet<>();
        cancellations.forEach((e, c) -> executors.get(e).cancel(c, succesfullyCancelled));

        for (CompactablePlan cplan : plans) {

          boolean cancelledAll = succesfullyCancelled.containsAll(
              Collections2.transform(cplan.plan.getCancellations(), Cancellation::getCompactionId));

          if (cancelledAll) {
            for (CompactionJob job : cplan.plan.getJobs()) {
              CompactionId compId = CompactionId.of(nextId++);
              SubmittedJob sjob =
                  executors.get(job.getExecutor()).submit(job, compId, cplan.compactable);
              submittedJobs.put(cplan.compactable.getExtent(), compId, sjob);
            }
          }
        }
      } catch (Exception e) {
        // TODO
        log.error("Loop failed ", e);
      }

      try {
        // TODO use sleep.. using wait because of sync
        wait(3000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      // UtilWaitThread.sleep(3000);// TODO configurable
    }
  }

  public CompactionManager(Iterable<Compactable> compactables) {
    this.compactables = compactables;
    // TODO confiugrable
    this.planner = new TieredCompactionManager(2);
    this.executors = Map.of("small", new CompactionExecutorImpl("small", 3), "medium",
        new CompactionExecutorImpl("medium", 3), "large", new CompactionExecutorImpl("large", 3),
        "huge", new CompactionExecutorImpl("huge", 2));
    this.submittedJobs = HashBasedTable.create();

  }

  public void start() {
    // TODO deamon thread
    // TODO stop method
    log.info("Started compaction manager");
    new Thread(() -> mainLoop()).start();

    // TODO remove
    new Thread(() -> printStats()).start();
  }
}
