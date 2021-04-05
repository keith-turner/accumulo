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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.ExternalCompactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadCompactionDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadCompactionDetector.class);

  private final ServerContext context;
  private final CompactionFinalizer finalizer;

  private static class CompIdExtent extends Pair<ExternalCompactionId,KeyExtent> {
    CompIdExtent(ExternalCompactionId ecid, KeyExtent extent) {
      super(ecid, extent);
    }

    public CompIdExtent(ExternalCompactionFinalState ecfs) {
      super(ecfs.getExternalCompactionId(), ecfs.getExtent());
    }
  }

  public DeadCompactionDetector(ServerContext context, CompactionFinalizer finalizer) {
    this.context = context;
    this.finalizer = finalizer;
  }

  private void detectDeadCompactions() {

    // The order of obtaining information is very important to avoid race conditions.

    log.trace("Starting to look for dead compactions");

    // find what external compactions tablets think are running
    Set<CompIdExtent> tabletCompactions = context.getAmple().readTablets().forLevel(DataLevel.USER)
        .fetch(ColumnType.ECOMP, ColumnType.PREV_ROW).build().stream()
        .flatMap(tm -> tm.getExternalCompactions().keySet().stream()
            .map(ecid -> new CompIdExtent(ecid, tm.getExtent())))
        .collect(Collectors.toSet());

    if (tabletCompactions.isEmpty()) {
      // no need to look for dead compactions when tablets don't have anything recorded as running
      return;
    }

    if(log.isTraceEnabled()) {
      tabletCompactions.forEach(cie -> log.trace("Saw {} for {}", cie.getFirst(), cie.getSecond()));
    }

    // Determine what compactions are currently running and remove those.
    //
    // In order for this overall algorithm to be correct and avoid race conditions, the compactor
    // must do two things when its asked what it is currently running.
    //
    // 1. If it is in the process of reserving a compaction, then it should wait for the
    // reservation to complete before responding to this request.
    // 2. If it is in the process of committing a compaction, then it should respond that it is
    // running the compaction it is currently committing.
    //
    // If the two conditions above are not met, then legitimate running compactions could be
    // canceled.
    Map<HostAndPort,TExternalCompactionJob> running =
        ExternalCompactionUtil.getCompactionsRunningOnCompactors(context);
    running.forEach((k, v) -> {
      CompIdExtent cie = new CompIdExtent(ExternalCompactionId.of(v.getExternalCompactionId()),
          KeyExtent.fromThrift(v.getExtent()));
      if(tabletCompactions.remove(cie)) {
        log.trace("Removed {} running on a compactor", cie.getFirst());
      }
    });

    // Determine which compactions are currently committing and remove those
    context.getAmple().getExternalCompactionFinalStates().map(CompIdExtent::new)
        .forEach(tabletCompactions::remove);

    tabletCompactions.forEach(
        cid -> log.debug("Detected dead compaction {} {}", cid.getFirst(), cid.getSecond()));

    // Everything left in tabletCompactions is no longer running anywhere and should be failed.
    // Its possible that a compaction committed while going through the steps above, if so then
    // that is ok and marking it failed will end up being a no-op.
    finalizer.failCompactions(tabletCompactions);
  }

  public void start() {
    Threads.createThread("DeadCompactionDetector", () -> {
      while (!Thread.currentThread().isInterrupted()) {

        try {
          detectDeadCompactions();
        } catch (RuntimeException e) {
          log.warn("Failed to look for dead compactions", e);
        }

        // TODO make bigger
        UtilWaitThread.sleep(30_000);
      }
    }).start();
  }
}
