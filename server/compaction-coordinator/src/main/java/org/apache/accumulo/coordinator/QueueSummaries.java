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
package org.apache.accumulo.coordinator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;

import com.google.common.collect.Sets;

public class QueueSummaries {

  // keep track of the last tserver retunred for qeueue
 final Map<String, PrioTserver> LAST = new HashMap<>();

 //CBUG may not need concurrent hash map depending on how sync is done
  /* Map of external queue name -> priority -> tservers */
  final Map<String,TreeMap<Long,TreeSet<TServerInstance>>> QUEUES =
      new ConcurrentHashMap<>();
  /* index of tserver to queue and priority, exists to provide O(1) lookup into QUEUES */
  final Map<TServerInstance,Set<QueueAndPriority>> INDEX =
      new ConcurrentHashMap<>();

  private Entry<Long,TreeSet<TServerInstance>> getNextTserverEntry(String queue) {
    TreeMap<Long,TreeSet<TServerInstance>> m = QUEUES.get(queue);
    if(m == null) {
      return null;
    }

    Iterator<Entry<Long,TreeSet<TServerInstance>>> iter = m.entrySet().iterator();

    while(iter.hasNext()) {
      Entry<Long,TreeSet<TServerInstance>> next = iter.next();
      if(next.getValue().isEmpty()) {
        iter.remove();
      } else {
        return next;
      }
    }

    QUEUES.remove(queue);

    return null;

  }


  static class PrioTserver {

    final TServerInstance tserver;
    final long prio;

    public PrioTserver(TServerInstance t, long p) {
      this.tserver = t;
      this.prio = p;
    }
  }

  //CBUG may want to to do finer grained sync...
  synchronized PrioTserver getNextTserver(String queue) {

    Entry<Long,TreeSet<TServerInstance>> entry = getNextTserverEntry(queue);

    if(entry == null) {
      // no tserver, so remove any last entry if it exists
      LAST.remove(queue);
      return null;
    }

    final Long priority = entry.getKey();
    final TreeSet<TServerInstance> tservers = entry.getValue();

    PrioTserver last = LAST.get(queue);

    TServerInstance nextTserver = null;

    if(last != null && last.prio == priority) {
      TServerInstance higher = tservers.higher(last.tserver);
      if(higher == null) {
        nextTserver = tservers.first();
      } else {
        nextTserver = higher;
      }
    } else {
      nextTserver = tservers.first();
    }

    PrioTserver result = new PrioTserver(nextTserver, priority);

    LAST.put(queue, result);

    return result;
  }


  synchronized void update(TServerInstance tsi, List<TCompactionQueueSummary> summaries) {

    Set<QueueAndPriority> newQP = new HashSet<>();
    summaries.forEach(summary -> {
      QueueAndPriority qp =
          QueueAndPriority.get(summary.getQueue().intern(), summary.getPriority());
      newQP.add(qp);
    });

    Set<QueueAndPriority> currentQP = INDEX.getOrDefault(tsi, Set.of());

    // remove anything the tserver did not report
    for(QueueAndPriority qp : Sets.difference(currentQP, newQP)) {
      removeSummary(tsi, qp.getQueue(), qp.getPriority());
    }

    INDEX.put(tsi, newQP);

    newQP.forEach(qp -> {
      QUEUES.computeIfAbsent(qp.getQueue(), k -> new TreeMap<>())
      .computeIfAbsent(qp.getPriority(), k -> new TreeSet<>()).add(tsi);
    });
  }

  synchronized void removeSummary(TServerInstance tsi, String queue, long priority) {
    TreeMap<Long,TreeSet<TServerInstance>> m = QUEUES.get(queue);
    if(m != null) {
      TreeSet<TServerInstance> s = m.get(priority);
      if(s != null) {
        if(s.remove(tsi) && s.isEmpty()) {
          m.remove(priority);
        }
      }

      if(m.isEmpty()) {
        QUEUES.remove(queue);
      }
    }

    Set<QueueAndPriority> qaps = INDEX.get(tsi);
    if(qaps != null) {
      if(qaps.remove(QueueAndPriority.get(queue, priority)) && qaps.isEmpty()) {
        INDEX.remove(tsi);
      }
    }
  }

  synchronized void remove(Set<TServerInstance> deleted) {
    deleted.forEach(tsi -> {
      INDEX.get(tsi).forEach(qp -> {
        TreeMap<Long,TreeSet<TServerInstance>> m = QUEUES.get(qp.getQueue());
        if (null != m) {
          TreeSet<TServerInstance> tservers = m.get(qp.getPriority());
          if (null != tservers) {
              tservers.remove(tsi);
          }
        }
      });
      INDEX.remove(tsi);
    });
  }
}
