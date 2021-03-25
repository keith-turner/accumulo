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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState.FinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionFinalizer {

  private static final Logger log = LoggerFactory.getLogger(CompactionFinalizer.class);

  private ServerContext context;
  private ExecutorService executor;
  private BlockingQueue<ExternalCompactionFinalState> pendingNotifications;

  CompactionFinalizer(ServerContext context) {
    this.context = context;
    this.pendingNotifications = new ArrayBlockingQueue<>(1000);
    // CBUG configure thread factory
    // CBUG make pool size configurable?
    int numThreads = 3;
    this.executor = Executors.newFixedThreadPool(numThreads + 2);

    executor.execute(() -> {
      processPending();
    });

    executor.execute(() -> {
      notifyTservers();
    });
  }

  public void commitCompaction(ExternalCompactionId ecid, KeyExtent extent, long fileSize,
      long fileEntries) {

    var ecfs =
        new ExternalCompactionFinalState(ecid, extent, FinalState.FINISHED, fileSize, fileEntries);

    // write metadata entry
    context.getAmple().putExternalCompactionFinalStates(List.of(ecfs));

    // queue RPC if queue is not full
    pendingNotifications.offer(ecfs);
  }

  public void
      failCompactions(Set<? extends Pair<ExternalCompactionId,KeyExtent>> compactionsToFail) {

    var finalStates =
        compactionsToFail.stream().map(ctf -> new ExternalCompactionFinalState(ctf.getFirst(),
            ctf.getSecond(), FinalState.FAILED, 0L, 0L)).collect(Collectors.toList());

    context.getAmple().putExternalCompactionFinalStates(finalStates);

    finalStates.forEach(pendingNotifications::offer);
  }

  private void notifyTserver(Location loc, ExternalCompactionFinalState ecfs) {

    TabletClientService.Client client = null;
    try {
      client = ThriftUtil.getClient(new TabletClientService.Client.Factory(), loc.getHostAndPort(),
          context);
      if (ecfs.getFinalState() == FinalState.FINISHED) {
        client.compactionJobFinished(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical(), ecfs.getFileSize(), ecfs.getEntries());
      } else if (ecfs.getFinalState() == FinalState.FAILED) {
        client.compactionJobFailed(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical());
      } else {
        throw new IllegalArgumentException(ecfs.getFinalState().name());
      }
    } catch (TException e) {
      log.debug("Failed to notify tserver {}", loc.getHostAndPort(), e);
    } finally {
      ThriftUtil.returnClient(client);
    }
  }

  private void processPending() {

    while (!Thread.interrupted()) {
      try {
        ArrayList<ExternalCompactionFinalState> batch = new ArrayList<>();
        batch.add(pendingNotifications.take());
        pendingNotifications.drainTo(batch);

        List<Future<?>> futures = new ArrayList<>();

        List<ExternalCompactionId> statusesToDelete = new ArrayList<>();

        for (ExternalCompactionFinalState ecfs : batch) {
          // CBUG use #1974 for more efficient metadata reads
          TabletMetadata tabletMetadata = context.getAmple().readTablets()
              .forTablet(ecfs.getExtent()).fetch(ColumnType.LOCATION, ColumnType.PREV_ROW).build()
              .stream().findFirst().orElse(null);

          if (tabletMetadata != null && tabletMetadata.getExtent().equals(ecfs.getExtent())) {
            var loc = tabletMetadata.getLocation();
            if (loc != null && loc.getType() == LocationType.CURRENT) {
              futures.add(executor.submit(() -> notifyTserver(loc, ecfs)));
            }

          } else {
            // this is an unknown tablet so need to delete its final state marker from metadata
            // table
            statusesToDelete.add(ecfs.getExternalCompactionId());
          }
        }

        if (!statusesToDelete.isEmpty()) {
          context.getAmple().deleteExternalCompactionFinalStates(statusesToDelete);
        }

        for (Future<?> future : futures) {
          try {
            future.get();
          } catch (ExecutionException e) {
            log.debug("Failed to notify tserver", e);
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        log.warn("Failed to process pending notifications", e);
      }
    }
  }

  private void notifyTservers() {
    while (!Thread.interrupted()) {

      try {
        Iterator<ExternalCompactionFinalState> finalStates =
            context.getAmple().getExternalCompactionFinalStates().iterator();
        while (finalStates.hasNext()) {
          pendingNotifications.put(finalStates.next());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        log.warn("Failed to notify tservers", e);
      }

      // CBUG make configurable?
      UtilWaitThread.sleep(60000);
    }
  }
}
