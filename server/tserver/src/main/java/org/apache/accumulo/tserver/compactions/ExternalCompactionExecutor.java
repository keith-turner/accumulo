package org.apache.accumulo.tserver.compactions;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;

public class ExternalCompactionExecutor implements CompactionExecutor {

  private class ExternalJob extends SubmittedJob implements Comparable<ExternalJob> {
    private AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);
    private Compactable compactable;
    private CompactionServiceId csid;
    private Consumer<Compactable> completionCallback;
    private final long queuedTime;

    public ExternalJob(CompactionJob job, Compactable compactable, CompactionServiceId csid,
        Consumer<Compactable> completionCallback) {
      super(job);
      this.compactable = compactable;
      this.csid = csid;
      this.completionCallback = completionCallback;
      queuedTime = System.currentTimeMillis();
    }

    @Override
    public Status getStatus() {
      return status.get();
    }

    @Override
    public boolean cancel(Status expectedStatus) {

      boolean canceled = false;

      if (expectedStatus == Status.QUEUED) {
        canceled = status.compareAndSet(expectedStatus, Status.CANCELED);
      }

      return canceled;
    }

    @Override
    public int compareTo(ExternalJob o) {
      return Long.compare(o.getJob().getPriority(), getJob().getPriority());
    }

  }


  private PriorityBlockingQueue<ExternalJob> queue;
  private CompactionExecutorId ceid;

  ExternalCompactionExecutor(){
    queue = new PriorityBlockingQueue<ExternalJob>();
  }

  public ExternalCompactionExecutor(CompactionExecutorId ceid) {
    this.ceid = ceid;
    this.queue = new PriorityBlockingQueue<ExternalJob>();
  }

  @Override
  public SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    ExternalJob extJob = new ExternalJob(job, compactable, csid, completionCallback);
    queue.add(extJob);
    return extJob;
  }

  @Override
  public int getCompactionsRunning() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getCompactionsQueued() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub

  }

  ExternalCompaction
      reserveExternalCompaction(long priority, String compactorId) {



    ExternalJob extJob = queue.poll();
    while(extJob.getStatus() != Status.QUEUED) {
      extJob = queue.poll();
    }

    if(extJob.getJob().getPriority() >= priority) {
      if(extJob.status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
        return extJob.compactable.reserveExternalCompaction(compactorId);
      } else {
        //TODO try again
      }
    } else {
      //TODO this messes with the ordering.. maybe make the comparator compare on time also
      queue.add(extJob);
    }


    // TODO Auto-generated method stub
    return null;
  }

}
