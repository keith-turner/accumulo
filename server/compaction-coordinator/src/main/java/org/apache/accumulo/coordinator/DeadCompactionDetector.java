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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.compaction.thrift.Compactor;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadCompactionDetector {

  private static final Logger log = LoggerFactory.getLogger(DeadCompactionDetector.class);

  private ServerContext context;
  private CompactionFinalizer finalizer;

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

  private List<HostAndPort> getCompactorAddrs() {
    try {
      List<HostAndPort> compactAddrs = new ArrayList<>();

      String compactorQueuesPath = context.getZooKeeperRoot() + Constants.ZCOMPACTORS;

      List<String> queues = context.getZooReaderWriter().getChildren(compactorQueuesPath);

      for (String queue : queues) {
        try {
          List<String> compactors =
              context.getZooReaderWriter().getChildren(compactorQueuesPath + "/" + queue);
          for (String compactor : compactors) {
            List<String> children = context.getZooReaderWriter()
                .getChildren(compactorQueuesPath + "/" + queue + "/" + compactor);
            if (!children.isEmpty()) {
              log.debug("Found live compactor {} ", compactor);
              compactAddrs.add(HostAndPort.fromString(compactor));
            }
          }
        } catch (NoNodeException e) {
          // CBUG change to trace
          log.debug("Ignoring node that went missing", e);
        }
      }

      return compactAddrs;
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private CompIdExtent getCompactionRunning(HostAndPort compactorAddr) {

    Compactor.Client client = null;
    try {
      // CBUG should this retry?
      client = ThriftUtil.getClient(new Compactor.Client.Factory(), compactorAddr, context);
      var rc = client.getRunningCompaction(TraceUtil.traceInfo(), context.rpcCreds());
      if (rc.externalCompactionId != null) {
        log.debug("Compactor is running {}", rc.externalCompactionId);
        return new CompIdExtent(ExternalCompactionId.of(rc.externalCompactionId),
            KeyExtent.fromThrift(rc.extent));
      }
    } catch (TException e) {
      log.debug("Failed to conact compactor {}", compactorAddr, e);
    } finally {
      ThriftUtil.returnClient(client);
    }

    return null;
  }

  private Stream<CompIdExtent> getCompactionsRunningOnCompactors() {
    ExecutorService executor = Executors.newFixedThreadPool(16);

    List<Future<CompIdExtent>> futures = new ArrayList<>();

    var compactorAddrs = getCompactorAddrs();

    for (HostAndPort compactorAddr : compactorAddrs) {
      futures.add(executor.submit(() -> getCompactionRunning(compactorAddr)));
    }

    executor.shutdown();

    return futures.stream().map(future -> {
      try {
        return future.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }).filter(cie -> cie != null);
  }

  private void detectDeadCompactions() {

    // The order of obtaining information is very important to avoid race conditions.

    // find what external comapctions tablets think are running
    Set<CompIdExtent> tabletCompactions = context.getAmple().readTablets().forLevel(DataLevel.USER)
        .fetch(ColumnType.ECOMP, ColumnType.PREV_ROW).build().stream()
        .flatMap(tm -> tm.getExternalCompactions().keySet().stream()
            .map(ecid -> new CompIdExtent(ecid, tm.getExtent())))
        .collect(Collectors.toSet());

    if (tabletCompactions.isEmpty()) {
      // no need to look for dead compactions when tablets don't have anything recorded as running
      return;
    }

    // Determine what compactions are currently running and remove those.
    //
    // Inorder for this overall algorithm to be correct and avoid race conditions, the compactor
    // must do two things when its asked what it is currently running.
    //
    // 1. If it is in the process of reserving a compaction, then it should wait for the
    // reservation to complete before responding to this request.
    // 2. If it is in the process of committing a compaction, then it should respond that it is
    // running the compaction it is currently committing.
    //
    // If the two conditions above are not met, then legitimate running compactions could be
    // canceled.
    getCompactionsRunningOnCompactors().forEach(tabletCompactions::remove);

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
    new Thread(() -> {
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
