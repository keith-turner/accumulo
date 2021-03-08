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

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
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

  ExternalCompactionExecutor() {
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

  ExternalCompactionJob reserveExternalCompaction(long priority, String compactorId) {

    ExternalJob extJob = queue.poll();
    while (extJob.getStatus() != Status.QUEUED) {
      extJob = queue.poll();
    }

    if (extJob.getJob().getPriority() >= priority) {
      if (extJob.status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
        return extJob.compactable.reserveExternalCompaction(extJob.csid, extJob.getJob(),
            compactorId);
      } else {
        // TODO try again
      }
    } else {
      // TODO this messes with the ordering.. maybe make the comparator compare on time also
      queue.add(extJob);
    }

    // TODO Auto-generated method stub
    return null;
  }

  // TODO maybe create non-thrift type to avoid thrift types all over the code
  public TCompactionQueueSummary summarize() {
    // TODO maybe try to keep this precomputed to avoid looping over entire queue for each request
    // TODO if count is not needed would not even need to loop over entire queue
    // TODO cast to int is problematic
    int count = (int) queue.stream().filter(extJob -> extJob.status.get() == Status.QUEUED).count();

    // TODO is there a better way to get prio w/o looping over everything? seems a stream over the
    // queue is not in particular order
    long priority = queue.stream().filter(extJob -> extJob.status.get() == Status.QUEUED)
        .mapToLong(extJob -> extJob.getJob().getPriority()).max().orElse(0);

    // TODO put extraction of queue name in one place
    return new TCompactionQueueSummary(ceid.canonical().substring(2), priority, count);
  }

}
