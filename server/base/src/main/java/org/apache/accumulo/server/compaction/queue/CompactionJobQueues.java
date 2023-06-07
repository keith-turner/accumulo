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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;

public class CompactionJobQueues {
  // TODO move to manager package
  // TODO would need to periodically clean out compaction queues that are no longer used.
  private final Map<CompactionExecutorId,CompactionJobPriorityQueue> priorityQueues =
      new ConcurrentHashMap<>();

  public void add(KeyExtent extent, Collection<CompactionJob> jobs) {
    if (jobs.size() == 1) {
      var executorId = jobs.iterator().next().getExecutor();
      add(extent, executorId, jobs);
    } else {
      jobs.stream().collect(Collectors.groupingBy(CompactionJob::getExecutor))
          .forEach(((executorId, compactionJobs) -> add(extent, executorId, compactionJobs)));
    }
  }

  public CompactionJob poll(CompactionExecutorId executorId) {
    var prioQ = priorityQueues.get(executorId);
    if (prioQ == null) {
      return null;
    }
    return prioQ.poll();
  }

  private void add(KeyExtent extent, CompactionExecutorId executorId,
      Collection<CompactionJob> jobs) {
    // TODO make max size configurable
    priorityQueues.computeIfAbsent(executorId, eid -> new CompactionJobPriorityQueue(eid, 10000))
        .add(extent, jobs);
  }

}
