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
package org.apache.accumulo.manager.compaction.queue;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionJobQueues {

  private static final Logger log = LoggerFactory.getLogger(CompactionJobQueues.class);

  private final ConcurrentHashMap<CompactionExecutorId,CompactionJobPriorityQueue> priorityQueues =
      new ConcurrentHashMap<>();

  public void add(TabletMetadata tabletMetadata, Collection<CompactionJob> jobs) {
    if (jobs.size() == 1) {
      var executorId = jobs.iterator().next().getExecutor();
      add(tabletMetadata, executorId, jobs);
    } else {
      jobs.stream().collect(Collectors.groupingBy(CompactionJob::getExecutor)).forEach(
          ((executorId, compactionJobs) -> add(tabletMetadata, executorId, compactionJobs)));
    }
  }

  public static class MetaJob {
    private final CompactionJob job;

    // the metadata from which the compaction job was derived
    private final TabletMetadata tabletMetadata;

    public MetaJob(CompactionJob job, TabletMetadata tabletMetadata) {
      this.job = job;
      this.tabletMetadata = tabletMetadata;
    }

    public CompactionJob getJob() {
      return job;
    }

    public TabletMetadata getTabletMetadata() {
      return tabletMetadata;
    }
  }

  public MetaJob poll(CompactionExecutorId executorId) {
    var prioQ = priorityQueues.get(executorId);
    if (prioQ == null) {
      return null;
    }
    MetaJob mj = prioQ.poll();

    if(mj == null){
      priorityQueues.computeIfPresent(executorId, (eid, pq)->{
        if(pq.closeIfEmpty()){
          return null;
        } else {
          return pq;
        }
      });
    }

    return mj;
  }

  private void add(TabletMetadata tabletMetadata, CompactionExecutorId executorId,
      Collection<CompactionJob> jobs) {

    // TODO log level
    if (log.isDebugEnabled()) {
      log.debug("Adding jobs to queue {} {} {}", executorId, tabletMetadata.getExtent(),
          jobs.stream().map(job -> "#files:" + job.getFiles().size() + ",prio:" + job.getPriority()
              + ",kind:" + job.getKind()).collect(Collectors.toList()));
    }

    // TODO make max size configurable
    var pq = priorityQueues.computeIfAbsent(executorId, eid -> new CompactionJobPriorityQueue(eid, 10000));
    while(!pq.add(tabletMetadata, jobs)){
      // This loop handles race condition where poll() closes empty priority queues.  The queue could be closed after its obtained from the map and before add is called.
      pq = priorityQueues.computeIfAbsent(executorId, eid -> new CompactionJobPriorityQueue(eid, 10000));
    }
  }
}
