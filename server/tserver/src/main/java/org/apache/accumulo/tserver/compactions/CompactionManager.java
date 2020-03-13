package org.apache.accumulo.tserver.compactions;

import java.util.Map;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SubmittedJob;
import org.apache.accumulo.core.spi.compaction.SubmittedJob.Status;

import com.google.common.collect.Iterables;
import com.google.common.collect.Table;

public class CompactionManager {

  private CompactionPlanner planner;
  private Iterable<Compactable> compactables;
  private Map<String,CompactionExecutor> executors;
  private Table<KeyExtent,CompactionId,SubmittedJob> submittedJobs;
  private long nextId = 0;

  private void mainLoop() {
    for (Compactable compactable : compactables) {

      // TODO look to avoid linear search
      submittedJobs.cellSet().removeIf(cell -> {
        var status = cell.getValue().getStatus();
        return status == Status.COMPLETE || status == Status.FAILED;
      });

      CompactionPlan plan = planner.makePlan(new PlanningParametersImpl(compactable,
          submittedJobs.row(compactable.getExtent()).values()));

      for (CompactionId cancelId : plan.cancellations) {
        SubmittedJob sjob = Iterables.getOnlyElement(submittedJobs.column(cancelId).values());
        executors.get(sjob.getJob().getExecutor()).cancel(cancelId);
        // TODO check if canceled
      }

      for (CompactionJob job : plan.jobs) {
        CompactionId compId = CompactionId.of(nextId++);
        SubmittedJob sjob = executors.get(job.getExecutor()).submit(job, compId);
        submittedJobs.put(compactable.getExtent(), compId, sjob);
      }

    }
  }
}
