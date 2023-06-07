/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.compaction.queue;

import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;

import com.google.common.base.Preconditions;

/**
 * Priority Queue for {@link CompactionJob}s that supports a maximum size. When a job is added and
 * the queue is at maximum size the new job is compared to the lowest job with the lowest priority.
 * The new job will either replace the lowest priority one or be ignored.
 *
 * <p>
 * When jobs are added for tablet, any previous jobs that are queued for the tablet are removed.
 * </p>
 */
public class CompactionJobPriorityQueue {
  // TODO unit test this class
  // TODO move this to manager package?

  private final CompactionExecutorId executorId;

  private class Entry implements Comparable<Entry> {
    private final CompactionJob job;

    // this exists to make every entry unique even if the job is the same, this is done because a
    // treeset is used as a queue
    private final long seq;

    // extent is intentionally not part of comparisonm hashCode, or equals. The extent is used to
    // find this object in the tabletJobs map.
    private final KeyExtent extent;

    Entry(CompactionJob job, KeyExtent extent) {
      this.job = job;
      this.seq = nextSeq++;
      this.extent = extent;
    }

    @Override
    public int compareTo(Entry oe) {

      int cmp = CompactionJobPrioritizer.JOB_COMPARATOR.compare(this.job, oe.job);
      if (cmp == 0) {
        cmp = Long.compare(seq, oe.seq);
      }
      return cmp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(job, seq);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      Entry entry = (Entry) o;
      return seq == entry.seq && job.equals(entry.job);
    }
  }

  // There are two reasons for using a TreeSet instead of a PriorityQueue. First the maximum size
  // behavior is not supported with a PriorityQueue. Second a PriorityQueue does not support
  // efficiently removing entries from anywhere in the queue. Efficient removal is needed for the
  // case where tablets decided to issues different compaction jobs than what is currently queued.
  private final TreeSet<Entry> jobQueue;
  private final int maxSize;

  // This map tracks what jobs a tablet currently has in the queue. Its used to efficiently remove
  // jobs in the queue when new jobs are queued for a tablet.
  private final Map<KeyExtent,List<Entry>> tabletJobs;

  private long nextSeq;

  public CompactionJobPriorityQueue(CompactionExecutorId executorId, int maxSize) {
    this.jobQueue = new TreeSet<>();
    this.maxSize = maxSize;
    this.tabletJobs = new HashMap<>();
    this.executorId = executorId;
  }

  public synchronized void add(KeyExtent extent, Collection<CompactionJob> jobs) {
    // this is important for reasoning about set operations
    Preconditions.checkArgument(jobs.stream().allMatch(job -> job instanceof CompactionJobImpl));
    Preconditions
        .checkArgument(jobs.stream().allMatch(job -> job.getExecutor().equals(executorId)));

    jobs = reconcileWithPrevSubmissions(extent, jobs);

    List<Entry> newEntries = new ArrayList<>(jobs.size());

    for (CompactionJob job : jobs) {
      Entry entry = addJobToQueue(extent, job);
      if (entry != null) {
        newEntries.add(entry);
      }
    }

    if (!newEntries.isEmpty()) {
      checkState(tabletJobs.put(extent, newEntries) == null);
    }

  }

  public synchronized CompactionJob poll() {
    Entry first = jobQueue.pollFirst();

    if (first != null) {
      List<Entry> jobs = tabletJobs.get(first.extent);
      checkState(jobs.remove(first));
      if (jobs.isEmpty()) {
        tabletJobs.remove(first.extent);
      }
    }

    return first == null ? null : first.job;
  }

  private Collection<CompactionJob> reconcileWithPrevSubmissions(KeyExtent extent,
      Collection<CompactionJob> jobs) {
    List<Entry> prevJobs = tabletJobs.get(extent);
    if (prevJobs != null) {
      // TODO instead of removing everything, attempt to detect if old and new jobs are the same
      prevJobs.forEach(jobQueue::remove);
      tabletJobs.remove(extent);
    }
    return jobs;
  }

  private Entry addJobToQueue(KeyExtent extent, CompactionJob job) {
    if (jobQueue.size() >= maxSize) {
      var lastEntry = jobQueue.last();
      if (job.getPriority() <= lastEntry.job.getPriority()) {
        // the queue is full and this job has a lower or same priority than the lowest job in the
        // queue, so do not add it
        return null;
      } else {
        // the new job has a higher priority than the lowest job in the queue, so remove the lowest
        jobQueue.pollLast();
      }

    }

    var entry = new Entry(job, extent);
    jobQueue.add(entry);
    return entry;
  }
}
