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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.spi.compaction.Cancellation;
import org.apache.accumulo.core.spi.compaction.CompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.SubmittedJob;

public class CompactionExecutor {

  private static class CompactionTask extends SubmittedJob implements Runnable {

    private AtomicReference<Status> status = new AtomicReference<>(Status.QUEUED);

    public CompactionTask(CompactionJob job, CompactionId id) {
      super(job, id);
      // TODO Auto-generated constructor stub
    }

    @Override
    public void run() {

      try {
        if (status.compareAndSet(Status.QUEUED, Status.RUNNING)) {

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

  CompactionExecutor() {
    var comparator = Comparator.comparingLong(CompactionExecutor::extractPriority)
        .thenComparingLong(CompactionExecutor::extractJobFiles).reversed();

    queue = new PriorityBlockingQueue<Runnable>(100, comparator);
  }

  public SubmittedJob submit(CompactionJob job, CompactionId compactionId) {
    return new CompactionTask(job, compactionId);
  }

  public void cancel(Cancellation cancelation) {

  }

}
