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

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.spi.compaction.Cancellation;
import org.apache.accumulo.core.spi.compaction.CompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.SubmittedJob;
import org.apache.accumulo.core.spi.compaction.SubmittedJob.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO rename to impl and extend CompactionExecutor?
public class CompactionExecutor {

  private static final Logger log = LoggerFactory.getLogger(CompactionExecutor.class);

  private static class CompactionTask extends SubmittedJob implements Runnable {

    private AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);
    private Compactable compactable;

    public CompactionTask(CompactionJob job, CompactionId id, Compactable compactable) {
      super(job, id);
      this.compactable = compactable;
    }

    @Override
    public void run() {

      try {
        if (status.compareAndSet(Status.QUEUED, Status.RUNNING)) {
          log.info(
              "Running compaction for " + compactable.getExtent() + " " + getJob().getExecutor());
          compactable.compact(getJob());
          log.info(
              "Finished compaction for " + compactable.getExtent() + " " + getJob().getExecutor());
        }
      } catch (Exception e) {
        // TODO
        e.printStackTrace();
        status.compareAndSet(Status.RUNNING, Status.FAILED);
      } finally {
        status.compareAndSet(Status.RUNNING, Status.COMPLETE);
      }

      // TODO Auto-generated method stub

    }

    @Override
    public Status getStatus() {
      return status.get();
    }

  }

  private static long extractPriority(Runnable r) {
    return ((CompactionTask) r).getJob().getPriority();
  }

  private static long extractJobFiles(Runnable r) {
    return ((CompactionTask) r).getJob().getFiles().size();
  }

  private PriorityBlockingQueue<Runnable> queue;
  private ThreadPoolExecutor executor;

  CompactionExecutor(int threads) {
    var comparator = Comparator.comparingLong(CompactionExecutor::extractPriority)
        .thenComparingLong(CompactionExecutor::extractJobFiles).reversed();

    queue = new PriorityBlockingQueue<Runnable>(100, comparator);

    executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, queue);

  }

  public SubmittedJob submit(CompactionJob job, CompactionId compactionId,
      Compactable compactable) {
    var ctask = new CompactionTask(job, compactionId, compactable);
    executor.execute(ctask);
    return ctask;
  }

  public void cancel(Cancellation cancelation) {
    // TODO avoid linear search
    queue.forEach(runnable -> {
      var ctask = (CompactionTask) runnable;
      if (ctask.getId().equals(cancelation.getCompactionId())) {
        cancelation.getStatusesToCancel()
            .forEach(status -> ctask.status.compareAndSet(status, Status.CANCELED));
        // TODO if running interrupt
      }
    });
  }
}
