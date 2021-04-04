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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;

public class ExternalCompactionExecutor implements CompactionExecutor {

  private Set<ExternalJob> queuedTask = Collections.synchronizedSet(new HashSet<>());

  private class ExternalJob extends SubmittedJob implements Comparable<ExternalJob> {
    private AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);
    private Compactable compactable;
    private CompactionServiceId csid;
    private volatile ExternalCompactionId ecid;

    public ExternalJob(CompactionJob job, Compactable compactable, CompactionServiceId csid) {
      super(job);
      this.compactable = compactable;
      this.csid = csid;
      queuedTask.add(this);
    }

    @Override
    public Status getStatus() {
      var s = status.get();
      if (s == Status.RUNNING && ecid != null && !compactable.isActive(ecid)) {
        s = Status.COMPLETE;
      }

      return s;
    }

    @Override
    public boolean cancel(Status expectedStatus) {

      boolean canceled = false;

      if (expectedStatus == Status.QUEUED) {
        canceled = status.compareAndSet(expectedStatus, Status.CANCELED);
        if (canceled) {
          queuedTask.remove(this);
        }
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
    ExternalJob extJob = new ExternalJob(job, compactable, csid);
    queue.add(extJob);
    return extJob;
  }

  @Override
  public int getCompactionsRunning(CType ctype) {
    if (ctype == CType.EXTERNAL)
      throw new UnsupportedOperationException();
    return 0;
  }

  @Override
  public int getCompactionsQueued(CType ctype) {
    if (ctype != CType.EXTERNAL)
      return 0;
    return queuedTask.size();
  }

  @Override
  public void stop() {}

  ExternalCompactionJob reserveExternalCompaction(long priority, String compactorId,
      ExternalCompactionId externalCompactionId) {

    ExternalJob extJob = queue.poll();
    while (extJob != null && extJob.getStatus() != Status.QUEUED) {
      extJob = queue.poll();
    }

    if (extJob == null) {
      return null;
    }

    if (extJob.getJob().getPriority() >= priority) {
      if (extJob.status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
        var ecj = extJob.compactable.reserveExternalCompaction(extJob.csid, extJob.getJob(),
            compactorId, externalCompactionId);
        extJob.ecid = ecj.getExternalCompactionId();
        queuedTask.remove(extJob);
        return ecj;
      } else {
        // TODO could this cause a stack overflow?
        return reserveExternalCompaction(priority, compactorId, externalCompactionId);
      }
    } else {
      // TODO this messes with the ordering.. maybe make the comparator compare on time also
      queue.add(extJob);
    }

    return null;
  }

  // TODO maybe create non-thrift type to avoid thrift types all over the code
  public TCompactionQueueSummary summarize() {
    int count = queuedTask.size();

    long priority = 0;
    ExternalJob topJob = queue.peek();
    while (topJob != null && topJob.getStatus() != Status.QUEUED) {
      queue.removeIf(extJob -> extJob.getStatus() != Status.QUEUED);
      topJob = queue.peek();
    }

    if (topJob != null) {
      priority = topJob.getJob().getPriority();
    }

    return new TCompactionQueueSummary(ceid.getExernalName(), priority, count);
  }

}
