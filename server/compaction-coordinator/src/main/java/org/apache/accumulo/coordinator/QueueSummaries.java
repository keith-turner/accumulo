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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;

import com.google.common.collect.Sets;

public class QueueSummaries {

  // keep track of the last tserver returned for queue
  final Map<String,PrioTserver> LAST = new HashMap<>();

  /* Map of external queue name -> priority -> tservers */
  final Map<String,TreeMap<Long,TreeSet<TServerInstance>>> QUEUES = new HashMap<>();
  /* index of tserver to queue and priority, exists to provide O(1) lookup into QUEUES */
  final Map<TServerInstance,Set<QueueAndPriority>> INDEX = new HashMap<>();

  private Entry<Long,TreeSet<TServerInstance>> getNextTserverEntry(String queue) {
    TreeMap<Long,TreeSet<TServerInstance>> m = QUEUES.get(queue);
    if (m == null) {
      return null;
    }

    Iterator<Entry<Long,TreeSet<TServerInstance>>> iter = m.entrySet().iterator();

    if (iter.hasNext()) {
      Entry<Long,TreeSet<TServerInstance>> next = iter.next();
      if (next.getValue().isEmpty()) {
        throw new IllegalStateException(
            "Unexpected empty tserver set for queue " + queue + " and prio " + next.getKey());
      }
      return next;
    }

    throw new IllegalStateException("Unexpected empty map for queue " + queue);
  }

  static class PrioTserver {
    TServerInstance tserver;
    final long prio;

    public PrioTserver(TServerInstance t, long p) {
      this.tserver = t;
      this.prio = p;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PrioTserver) {
        PrioTserver opt = (PrioTserver) obj;
        return tserver.equals(opt.tserver) && prio == opt.prio;
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tserver, prio);
    }

    @Override
    public String toString() {
      return tserver + " " + prio;
    }
  }

  synchronized PrioTserver getNextTserver(String queue) {

    Entry<Long,TreeSet<TServerInstance>> entry = getNextTserverEntry(queue);

    if (entry == null) {
      // no tserver, so remove any last entry if it exists
      LAST.remove(queue);
      return null;
    }

    final Long priority = entry.getKey();
    final TreeSet<TServerInstance> tservers = entry.getValue();

    PrioTserver last = LAST.get(queue);

    TServerInstance nextTserver = null;

    if (last != null && last.prio == priority) {
      TServerInstance higher = tservers.higher(last.tserver);
      if (higher == null) {
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
    for (QueueAndPriority qp : List.copyOf(Sets.difference(currentQP, newQP))) {
      removeSummary(tsi, qp.getQueue(), qp.getPriority());
    }

    INDEX.put(tsi, newQP);

    newQP.forEach(qp -> {
      QUEUES.computeIfAbsent(qp.getQueue(), k -> new TreeMap<>(Comparator.reverseOrder()))
          .computeIfAbsent(qp.getPriority(), k -> new TreeSet<>()).add(tsi);
    });
  }

  synchronized void removeSummary(TServerInstance tsi, String queue, long priority) {
    TreeMap<Long,TreeSet<TServerInstance>> m = QUEUES.get(queue);
    if (m != null) {
      TreeSet<TServerInstance> s = m.get(priority);
      if (s != null) {
        if (s.remove(tsi) && s.isEmpty()) {
          m.remove(priority);
        }
      }

      if (m.isEmpty()) {
        QUEUES.remove(queue);
      }
    }

    Set<QueueAndPriority> qaps = INDEX.get(tsi);
    if (qaps != null) {
      if (qaps.remove(QueueAndPriority.get(queue, priority)) && qaps.isEmpty()) {
        INDEX.remove(tsi);
      }
    }
  }

  synchronized void remove(Set<TServerInstance> deleted) {
    deleted.forEach(tsi -> {
      INDEX.getOrDefault(tsi, Set.of()).forEach(qp -> {
        TreeMap<Long,TreeSet<TServerInstance>> m = QUEUES.get(qp.getQueue());
        if (null != m) {
          TreeSet<TServerInstance> tservers = m.get(qp.getPriority());
          if (null != tservers) {
            if (tservers.remove(tsi) && tservers.isEmpty()) {
              m.remove(qp.getPriority());
            }

            if (m.isEmpty()) {
              QUEUES.remove(qp.getQueue());
            }
          }
        }
      });
      INDEX.remove(tsi);
    });
  }
}