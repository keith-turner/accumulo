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
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionFinalizer {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionFinalizer.class);

  private final ServerContext context;
  private final ExecutorService ntfyExecutor;
  private final ExecutorService backgroundExecutor;
  private final BlockingQueue<ExternalCompactionFinalState> pendingNotifications;

  CompactionFinalizer(ServerContext context) {
    this.context = context;
    this.pendingNotifications = new ArrayBlockingQueue<>(1000);
    // CBUG configure thread factory
    // CBUG make pool size configurable?

    this.ntfyExecutor =
        ThreadPools.createFixedThreadPool(3, "Compaction Finalizer Notifyer", false);

    this.backgroundExecutor =
        ThreadPools.createFixedThreadPool(2, "Compaction Finalizer Background Task", false);

    backgroundExecutor.execute(() -> {
      processPending();
    });

    backgroundExecutor.execute(() -> {
      notifyTservers();
    });
  }

  public void commitCompaction(ExternalCompactionId ecid, KeyExtent extent, long fileSize,
      long fileEntries) {

    var ecfs =
        new ExternalCompactionFinalState(ecid, extent, FinalState.FINISHED, fileSize, fileEntries);

    // write metadata entry
    LOG.info("Writing completed external compaction to metadata table: {}", ecfs);
    context.getAmple().putExternalCompactionFinalStates(List.of(ecfs));

    // queue RPC if queue is not full
    LOG.info("Queueing tserver notification for completed external compaction: {}", ecfs);
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
        LOG.info("Notifying tserver {} that compaction {} has finished.", loc, ecfs);
        client.compactionJobFinished(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical(), ecfs.getFileSize(), ecfs.getEntries());
      } else if (ecfs.getFinalState() == FinalState.FAILED) {
        LOG.info("Notifying tserver {} that compaction {} has failed.", loc, ecfs);
        client.compactionJobFailed(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical());
      } else {
        throw new IllegalArgumentException(ecfs.getFinalState().name());
      }
    } catch (TException e) {
      LOG.warn("Failed to notify tserver {}", loc.getHostAndPort(), e);
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

          if (tabletMetadata == null || !tabletMetadata.getExtent().equals(ecfs.getExtent())) {
            // this is an unknown tablet so need to delete its final state marker from metadata
            // table
            LOG.debug(
                "Unable to find tserver for external compaction {}, deleting completion entry",
                ecfs);
            statusesToDelete.add(ecfs.getExternalCompactionId());
          } else if (tabletMetadata.getLocation() != null
              && tabletMetadata.getLocation().getType() == LocationType.CURRENT) {
            futures
                .add(ntfyExecutor.submit(() -> notifyTserver(tabletMetadata.getLocation(), ecfs)));
          } else {
            LOG.trace(
                "External compaction {} is completed, but there is no location for tablet.  Unable to notify tablet, will try again later.",
                ecfs);
          }
        }

        if (!statusesToDelete.isEmpty()) {
          LOG.info(
              "Deleting unresolvable completed external compactions from metadata table, ids: {}",
              statusesToDelete);
          context.getAmple().deleteExternalCompactionFinalStates(statusesToDelete);
        }

        for (Future<?> future : futures) {
          try {
            future.get();
          } catch (ExecutionException e) {
            LOG.debug("Failed to notify tserver", e);
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        LOG.warn("Failed to process pending notifications", e);
      }
    }
  }

  private void notifyTservers() {
    while (!Thread.interrupted()) {
      try {
        Iterator<ExternalCompactionFinalState> finalStates =
            context.getAmple().getExternalCompactionFinalStates().iterator();
        while (finalStates.hasNext()) {
          ExternalCompactionFinalState state = finalStates.next();
          LOG.info(
              "Found external compaction in final state: {}, queueing for tserver notification",
              state);
          pendingNotifications.put(state);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        LOG.warn("Failed to notify tservers", e);
      }

      // CBUG make configurable?
      UtilWaitThread.sleep(CompactionCoordinator.TSERVER_CHECK_INTERVAL);
    }
  }
}
