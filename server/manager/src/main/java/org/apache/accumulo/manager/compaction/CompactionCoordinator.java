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
package org.apache.accumulo.manager.compaction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.queue.CompactionJobQueues;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class CompactionCoordinator implements CompactionCoordinatorService.Iface, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCoordinator.class);
  private static final long FIFTEEN_MINUTES = TimeUnit.MINUTES.toMillis(15);

  /*
   * Map of compactionId to RunningCompactions. This is an informational cache of what external
   * compactions may be running. Its possible it may contain external compactions that are not
   * actually running. It may not contain compactions that are actually running. The metadata table
   * is the most authoritative source of what external compactions are currently running, but it
   * does not have the stats that this map has.
   */
  protected static final Map<ExternalCompactionId,RunningCompaction> RUNNING_CACHE =
      new ConcurrentHashMap<>();

  private static final Cache<ExternalCompactionId,RunningCompaction> COMPLETED =
      Caffeine.newBuilder().maximumSize(200).expireAfterWrite(10, TimeUnit.MINUTES).build();

  /* Map of queue name to last time compactor called to get a compaction job */
  // TODO need to clean out queues that are no longer configured..
  private static final Map<String,Long> TIME_COMPACTOR_LAST_CHECKED = new ConcurrentHashMap<>();

  private final ServerContext ctx;
  private final LiveTServerSet tserverSet;
  private final SecurityOperation security;
  private final CompactionJobQueues jobQueues;
  private CompactionFinalizer compactionFinalizer;

  // Exposed for tests
  protected volatile Boolean shutdown = false;

  private final ScheduledThreadPoolExecutor schedExecutor;

  public CompactionCoordinator(ServerContext ctx, LiveTServerSet tservers,
      SecurityOperation security, CompactionJobQueues jobQueues) {
    this.ctx = ctx;
    this.tserverSet = tservers;
    this.schedExecutor = this.ctx.getScheduledExecutor();
    this.security = security;
    this.jobQueues = jobQueues;
    createCompactionFinalizer(schedExecutor);
    startCompactionCleaner(schedExecutor);
    startRunningCleaner(schedExecutor);
  }

  public void shutdown() {
    shutdown = true;
  }

  protected void createCompactionFinalizer(ScheduledThreadPoolExecutor schedExecutor) {
    this.compactionFinalizer = new CompactionFinalizer(this.ctx, schedExecutor);
  }

  protected void startCompactionCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::cleanUpCompactors, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startRunningCleaner(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(this::cleanUpRunning, 0, 5, TimeUnit.MINUTES);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  @Override
  public void run() {

    // On a re-start of the coordinator it's possible that external compactions are in-progress.
    // Attempt to get the running compactions on the compactors and then resolve which tserver
    // the external compaction came from to re-populate the RUNNING collection.
    LOG.info("Checking for running external compactions");
    // On re-start contact the running Compactors to try and seed the list of running compactions
    List<RunningCompaction> running = getCompactionsRunningOnCompactors();
    if (running.isEmpty()) {
      LOG.info("No running external compactions found");
    } else {
      LOG.info("Found {} running external compactions", running.size());
      running.forEach(rc -> {
        TCompactionStatusUpdate update = new TCompactionStatusUpdate();
        update.setState(TCompactionState.IN_PROGRESS);
        update.setMessage("Coordinator restarted, compaction found in progress");
        rc.addUpdate(System.currentTimeMillis(), update);
        RUNNING_CACHE.put(ExternalCompactionId.of(rc.getJob().getExternalCompactionId()), rc);
      });
    }

    startDeadCompactionDetector();

    LOG.info("Starting loop to check tservers for compaction summaries");
    while (!shutdown) {
      long start = System.currentTimeMillis();

      long now = System.currentTimeMillis();
      TIME_COMPACTOR_LAST_CHECKED.forEach((k, v) -> {
        if ((now - v) > getMissingCompactorWarningTime()) {
          // TODO may want to consider of the queue has any jobs queued
          LOG.warn("No compactors have checked in with coordinator for queue {} in {}ms", k,
              getMissingCompactorWarningTime());
        }
      });

      long checkInterval = getTServerCheckInterval();
      long duration = (System.currentTimeMillis() - start);
      if (checkInterval - duration > 0) {
        // TODO this log message is wrong
        LOG.debug("Waiting {}ms for next tserver check", (checkInterval - duration));
        UtilWaitThread.sleep(checkInterval - duration);
      }
    }

    LOG.info("Shutting down");
  }

  protected void startDeadCompactionDetector() {
    new DeadCompactionDetector(this.ctx, this, schedExecutor).start();
  }

  protected long getMissingCompactorWarningTime() {
    return FIFTEEN_MINUTES;
  }

  protected long getTServerCheckInterval() {
    return this.ctx.getConfiguration()
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_TSERVER_COMPACTION_CHECK_INTERVAL);
  }

  /**
   * Callback for the LiveTServerSet object to update current set of tablet servers, including ones
   * that were deleted and added
   *
   * @param current current set of live tservers
   * @param deleted set of tservers that were removed from current since last update
   * @param added set of tservers that were added to current since last update
   */
  public void updateTServerSet(LiveTServerSet current, Set<TServerInstance> deleted,
      Set<TServerInstance> added) {

  }

  /**
   * Return the next compaction job from the queue to a Compactor
   *
   * @param queueName queue
   * @param compactorAddress compactor address
   * @throws ThriftSecurityException when permission error
   * @return compaction job
   */
  @Override
  public TExternalCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String queueName, String compactorAddress, String externalCompactionId)
      throws ThriftSecurityException {

    // do not expect users to call this directly, expect compactors to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final String queue = queueName.intern();
    LOG.trace("getCompactionJob called for queue {} by compactor {}", queue, compactorAddress);
    TIME_COMPACTOR_LAST_CHECKED.put(queue, System.currentTimeMillis());

    TExternalCompactionJob result = null;

    // TODO does this queue name line up with whats in the jobQueue
    CompactionJobQueues.MetaJob metaJob =
        jobQueues.poll(CompactionExecutorIdImpl.externalId(queueName));

    if (metaJob != null) {
      ExternalCompactionMetadata ecm =
          reserveCompaction(metaJob, compactorAddress, externalCompactionId);

      if (ecm != null) {
        result = createThriftJob(externalCompactionId, ecm, metaJob);
        // It is possible that by the time this added that the the compactor that made this request
        // is dead. In this cases the compaction is not actually running.
        RUNNING_CACHE.put(ExternalCompactionId.of(result.getExternalCompactionId()),
            new RunningCompaction(result, compactorAddress, queue));
        LOG.debug("Returning external job {} to {}", result.externalCompactionId, compactorAddress);
      } else {
        LOG.debug("Unable to reserve compaction for {} ", metaJob.getTabletMetadata().getExtent());
      }
      // create TExternalCompactionJob if above is successful and return it
    } else {
      LOG.debug("No jobs found in queue {} ", queue);
    }

    if (result == null) {
      LOG.trace("No tservers found for queue {}, returning empty job to compactor {}", queue,
          compactorAddress);
      result = new TExternalCompactionJob();
    }

    return result;

  }

  private ExternalCompactionMetadata reserveCompaction(CompactionJobQueues.MetaJob metaJob,
      String compactorAddress, String externalCompactionId) {

    // only handle system ATM
    Preconditions.checkArgument(metaJob.getJob().getKind() == CompactionKind.SYSTEM);

    var jobFiles = metaJob.getJob().getFiles().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());

    // TODO can probably remove this when selected files are stored in metadata
    Set<StoredTabletFile> nextFiles = Set.of();

    // TODO maybe structure code to where this can be unit tested
    boolean compactingAll = metaJob.getTabletMetadata().getFiles().equals(jobFiles);

    boolean propDels = !compactingAll;

    ReferencedTabletFile newFile =
        TabletNameGenerator.getNextDataFilenameForMajc(propDels, ctx, metaJob.getTabletMetadata());

    // TODO
    boolean initiallSelAll = false;

    Long compactionId = null;

    ExternalCompactionMetadata ecm = new ExternalCompactionMetadata(jobFiles, nextFiles, newFile,
        compactorAddress, metaJob.getJob().getKind(), metaJob.getJob().getPriority(),
        metaJob.getJob().getExecutor(), propDels, initiallSelAll, compactionId);

    try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
      var extent = metaJob.getTabletMetadata().getExtent();

      // TODO need a more complex conditional check that allows multiple concurrenct compactions...
      // need to check that this new compaction has disjoint files with any existing compactions
      var tabletMutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
          .requireAbsentCompactions().requirePrevEndRow(extent.prevEndRow());
      jobFiles.forEach(tabletMutator::requireFile);

      var ecid = ExternalCompactionId.of(externalCompactionId);
      tabletMutator.putExternalCompaction(ecid, ecm);

      tabletMutator
          .submit(tabletMetadata -> tabletMetadata.getExternalCompactions().containsKey(ecid));

      if (tabletsMutator.process().get(extent).getStatus()
          == Ample.ConditionalResult.Status.ACCEPTED) {
        return ecm;
      } else {
        // TODO could log tablet metadata and compaction job in this case, maybe at trace
        return null;
      }
    }

  }

  TExternalCompactionJob createThriftJob(String externalCompactionId,
      ExternalCompactionMetadata ecm, CompactionJobQueues.MetaJob metaJob) {
    // TODO delete class ExternalCompactionJob
    // TODO review all thrift stuff related to compactions, need to eventually delete stuff
    // TODO need to get the files time below

    // TODO get iterator config.. is this only needed for user compactions that pass iters?
    IteratorConfig iteratorSettings = SystemIteratorUtil.toIteratorConfig(List.of());

    var files = ecm.getJobFiles().stream().map(storedTabletFile -> {
      var dfv = metaJob.getTabletMetadata().getFilesMap().get(storedTabletFile);
      return new InputFile(storedTabletFile.getNormalizedPathStr(), dfv.getSize(),
          dfv.getNumEntries(), dfv.getTime());
    }).collect(Collectors.toList());

    // TODO will need to compute this
    Map<String,String> overrides = Map.of();

    return new TExternalCompactionJob(externalCompactionId,
        metaJob.getTabletMetadata().getExtent().toThrift(), files, iteratorSettings,
        ecm.getCompactTmpName().getNormalizedPathStr(), ecm.getPropagateDeletes(),
        TCompactionKind.valueOf(ecm.getKind().name()),
        ecm.getCompactionId() == null ? 0 : ecm.getCompactionId(), overrides);
  };

  /**
   * Compactor calls compactionCompleted passing in the CompactionStats
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @param externalCompactionId compaction id
   * @param textent tablet extent
   * @param stats compaction stats
   * @throws ThriftSecurityException when permission error
   */
  @Override
  public void compactionCompleted(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent textent, TCompactionStats stats)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    var extent = KeyExtent.fromThrift(textent);
    LOG.info("Compaction completed, id: {}, stats: {}, extent: {}", externalCompactionId, stats,
        extent);
    final var ecid = ExternalCompactionId.of(externalCompactionId);

    // TODO could maybe cache this info...
    var tabletMeta = ctx.getAmple().readTablet(extent, TabletMetadata.ColumnType.ECOMP);

    ExternalCompactionMetadata ecm = tabletMeta.getExternalCompactions().get(ecid);

    ReferencedTabletFile newDatafile =
        TabletNameGenerator.computeCompactionFileDest(ecm.getCompactTmpName());

    try {
      // TODO Check return value
      ctx.getVolumeManager().rename(ecm.getCompactTmpName().getPath(), newDatafile.getPath());
    } catch (IOException e) {
      // TODO log instead of throw exception
      throw new RuntimeException(e);
    }

    try (var tabletsMutator = ctx.getAmple().conditionallyMutateTablets()) {
      var tabletMutator = tabletsMutator.mutateTablet(extent).requireAbsentOperation()
          .requirePrevEndRow(extent.prevEndRow()).requireCompaction(ecid);
      ecm.getJobFiles().forEach(tabletMutator::requireFile);
      ecm.getJobFiles().forEach(tabletMutator::deleteFile);
      tabletMutator.deleteExternalCompaction(ecid);
      tabletMutator.putFile(newDatafile,
          new DataFileValue(stats.getFileSize(), stats.getEntriesWritten()));

      // TODO do something in rejection handler
      tabletMutator.submit(tabletMetadata -> false);

      // TODO check return value
      tabletsMutator.process();

      // TODO need to reliably notify tablet, may be able to do this in finalizer
    }

    // compactionFinalizer.commitCompaction(ecid, extent, stats.fileSize, stats.entriesWritten);

    // It's possible that RUNNING might not have an entry for this ecid in the case
    // of a coordinator restart when the Coordinator can't find the TServer for the
    // corresponding external compaction.
    recordCompletion(ecid);
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent) throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction failed, id: {}", externalCompactionId);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    compactionFailed(Map.of(ecid, KeyExtent.fromThrift(extent)));

    // TODO need to open an issue about making the GC clean up tmp files. The tablet currently
    // cleans up tmp files on tablet load. With tablets never loading possibly but still compacting
    // dying compactors may still leave tmp files behind.
  }

  void compactionFailed(Map<ExternalCompactionId,KeyExtent> compactions) {
    compactionFinalizer.failCompactions(compactions);
    compactions.forEach((k, v) -> recordCompletion(k));
  }

  /**
   * Compactor calls to update the status of the assigned compaction
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @param externalCompactionId compaction id
   * @param update compaction status update
   * @param timestamp timestamp of the message
   * @throws ThriftSecurityException when permission error
   */
  @Override
  public void updateCompactionStatus(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TCompactionStatusUpdate update, long timestamp)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.debug("Compaction status update, id: {}, timestamp: {}, update: {}", externalCompactionId,
        timestamp, update);
    final RunningCompaction rc = RUNNING_CACHE.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.addUpdate(timestamp, update);
    }
  }

  private void recordCompletion(ExternalCompactionId ecid) {
    var rc = RUNNING_CACHE.remove(ecid);
    if (rc != null) {
      COMPLETED.put(ecid, rc);
    }
  }

  protected Set<ExternalCompactionId> readExternalCompactionIds() {
    return this.ctx.getAmple().readTablets().forLevel(Ample.DataLevel.USER)
        .fetch(TabletMetadata.ColumnType.ECOMP).build().stream()
        .flatMap(tm -> tm.getExternalCompactions().keySet().stream()).collect(Collectors.toSet());
  }

  /**
   * The RUNNING_CACHE set may contain external compactions that are not actually running. This
   * method periodically cleans those up.
   */
  protected void cleanUpRunning() {

    // grab a snapshot of the ids in the set before reading the metadata table. This is done to
    // avoid removing things that are added while reading the metadata.
    Set<ExternalCompactionId> idsSnapshot = Set.copyOf(RUNNING_CACHE.keySet());

    // grab the ids that are listed as running in the metadata table. It important that this is done
    // after getting the snapshot.
    Set<ExternalCompactionId> idsInMetadata = readExternalCompactionIds();

    var idsToRemove = Sets.difference(idsSnapshot, idsInMetadata);

    // remove ids that are in the running set but not in the metadata table
    idsToRemove.forEach(ecid -> recordCompletion(ecid));

    if (idsToRemove.size() > 0) {
      LOG.debug("Removed stale entries from RUNNING_CACHE : {}", idsToRemove);
    }
  }

  /**
   * Return information about running compactions
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @return map of ECID to TExternalCompaction objects
   * @throws ThriftSecurityException permission error
   */
  @Override
  public TExternalCompactionList getRunningCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    final TExternalCompactionList result = new TExternalCompactionList();
    RUNNING_CACHE.forEach((ecid, rc) -> {
      TExternalCompaction trc = new TExternalCompaction();
      trc.setQueueName(rc.getQueueName());
      trc.setCompactor(rc.getCompactorAddress());
      trc.setUpdates(rc.getUpdates());
      trc.setJob(rc.getJob());
      result.putToCompactions(ecid.canonical(), trc);
    });
    return result;
  }

  /**
   * Return information about recently completed compactions
   *
   * @param tinfo trace info
   * @param credentials tcredentials object
   * @return map of ECID to TExternalCompaction objects
   * @throws ThriftSecurityException permission error
   */
  @Override
  public TExternalCompactionList getCompletedCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final TExternalCompactionList result = new TExternalCompactionList();
    COMPLETED.asMap().forEach((ecid, rc) -> {
      TExternalCompaction trc = new TExternalCompaction();
      trc.setQueueName(rc.getQueueName());
      trc.setCompactor(rc.getCompactorAddress());
      trc.setJob(rc.getJob());
      trc.setUpdates(rc.getUpdates());
      result.putToCompactions(ecid.canonical(), trc);
    });
    return result;
  }

  @Override
  public void cancel(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    var runningCompaction = RUNNING_CACHE.get(ExternalCompactionId.of(externalCompactionId));
    var extent = KeyExtent.fromThrift(runningCompaction.getJob().getExtent());
    try {
      NamespaceId nsId = this.ctx.getNamespaceId(extent.tableId());
      if (!security.canCompact(credentials, extent.tableId(), nsId)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(extent.tableId().canonical(), null,
          TableOperation.COMPACT_CANCEL, TableOperationExceptionType.NOTFOUND, e.getMessage());
    }

    cancelCompactionOnCompactor(runningCompaction.getCompactorAddress(), externalCompactionId);
  }

  /* Method exists to be overridden in test to hide static method */
  protected String getTServerAddressString(HostAndPort tserverAddress) {
    return ExternalCompactionUtil.getHostPortString(tserverAddress);
  }

  /* Method exists to be overridden in test to hide static method */
  protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
    return ExternalCompactionUtil.getCompactionsRunningOnCompactors(this.ctx);
  }

  /* Method exists to be overridden in test to hide static method */
  protected void cancelCompactionOnCompactor(String address, String externalCompactionId) {
    HostAndPort hostPort = HostAndPort.fromString(address);
    ExternalCompactionUtil.cancelCompaction(this.ctx, hostPort, externalCompactionId);
  }

  /* Method exists to be overridden in test to hide static method */
  protected void returnTServerClient(TabletServerClientService.Client client) {
    ThriftUtil.returnClient(client, this.ctx);
  }

  private void deleteEmpty(ZooReaderWriter zoorw, String path)
      throws KeeperException, InterruptedException {
    try {
      LOG.debug("Deleting empty ZK node {}", path);
      zoorw.delete(path);
    } catch (KeeperException.NotEmptyException e) {
      LOG.debug("Failed to delete {} its not empty, likely an expected race condition.", path);
    }
  }

  private void cleanUpCompactors() {
    final String compactorQueuesPath = this.ctx.getZooKeeperRoot() + Constants.ZCOMPACTORS;

    var zoorw = this.ctx.getZooReaderWriter();

    try {
      var queues = zoorw.getChildren(compactorQueuesPath);

      for (String queue : queues) {
        String qpath = compactorQueuesPath + "/" + queue;

        var compactors = zoorw.getChildren(qpath);

        if (compactors.isEmpty()) {
          deleteEmpty(zoorw, qpath);
        }

        for (String compactor : compactors) {
          String cpath = compactorQueuesPath + "/" + queue + "/" + compactor;
          var lockNodes = zoorw.getChildren(compactorQueuesPath + "/" + queue + "/" + compactor);
          if (lockNodes.isEmpty()) {
            deleteEmpty(zoorw, cpath);
          }
        }
      }

    } catch (KeeperException | RuntimeException e) {
      LOG.warn("Failed to clean up compactors", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

}
