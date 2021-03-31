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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ThriftTransportPool;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Iface;
import org.apache.accumulo.core.compaction.thrift.CompactionState;
import org.apache.accumulo.core.compaction.thrift.Compactor;
import org.apache.accumulo.core.compaction.thrift.Status;
import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.CompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.compaction.RetryableThriftCall;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.compaction.RetryableThriftFunction;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionCoordinator extends AbstractServer
    implements org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Iface,
    LiveTServerSet.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionCoordinator.class);
  private static final long TIME_BETWEEN_CHECKS = 5000;
  public static final long TSERVER_CHECK_INTERVAL = 60000;

  /* Map of external queue name -> priority -> tservers */
  private static final Map<String,TreeMap<Long,LinkedHashSet<TServerInstance>>> QUEUES =
      new ConcurrentHashMap<>();
  /* index of tserver to queue and priority, exists to provide O(1) lookup into QUEUES */
  private static final Map<TServerInstance,HashSet<QueueAndPriority>> INDEX =
      new ConcurrentHashMap<>();
  /* Map of compactionId to RunningCompactions */
  private static final Map<ExternalCompactionId,RunningCompaction> RUNNING =
      new ConcurrentHashMap<>();

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  private final AccumuloConfiguration aconf;
  private final CompactionFinalizer compactionFinalizer;
  private final LiveTServerSet tserverSet;
  private final SecurityOperation security;

  private ZooLock coordinatorLock;

  protected CompactionCoordinator(ServerOpts opts, String[] args) {
    super("compaction-coordinator", opts, args);
    ServerContext context = getContext();
    context.setupCrypto();

    compactionFinalizer = new CompactionFinalizer(context);
    tserverSet = new LiveTServerSet(context, this);
    security = AuditedSecurityOperation.getInstance(getContext());

    aconf = getConfiguration();
    ThreadPools.createGeneralScheduledExecutorService(aconf).scheduleWithFixedDelay(
        () -> gcLogger.logGCInfo(getConfiguration()), 0, TIME_BETWEEN_CHECKS,
        TimeUnit.MILLISECONDS);
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + context.getInstanceID());

  }

  /**
   * Set up nodes and locks in ZooKeeper for this CompactionCoordinator
   *
   * @param clientAddress
   *          address of this Compactor
   * @return true if lock was acquired, else false
   * @throws KeeperException
   * @throws InterruptedException
   */
  protected boolean getCoordinatorLock(HostAndPort clientAddress)
      throws KeeperException, InterruptedException {
    LOG.info("trying to get coordinator lock");

    final String coordinatorClientAddress = ExternalCompactionUtil.getHostPortString(clientAddress);
    final String lockPath = getContext().getZooKeeperRoot() + Constants.ZCOORDINATOR_LOCK;
    final UUID zooLockUUID = UUID.randomUUID();

    CoordinatorLockWatcher coordinatorLockWatcher = new CoordinatorLockWatcher();
    coordinatorLock = new ZooLock(getContext().getSiteConfiguration(), lockPath, zooLockUUID);
    // TODO may want to wait like manager code when lock not acquired, this allows starting multiple
    // coordinators.
    return coordinatorLock.tryLock(coordinatorLockWatcher, coordinatorClientAddress.getBytes());
  }

  /**
   * Start this CompactionCoordinator thrift service to handle incoming client requests
   *
   * @return address of this CompactionCoordinator client service
   * @throws UnknownHostException
   */
  protected ServerAddress startCoordinatorClientService() throws UnknownHostException {
    Iface rpcProxy = TraceUtil.wrapService(this);
    if (getContext().getThriftServerType() == ThriftServerType.SASL) {
      rpcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, CompactionCoordinator.class,
          getConfiguration());
    }
    final org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Processor<
        Iface> processor =
            new org.apache.accumulo.core.compaction.thrift.CompactionCoordinator.Processor<>(
                rpcProxy);
    Property maxMessageSizeProperty = (aconf.get(Property.COORDINATOR_MAX_MESSAGE_SIZE) != null
        ? Property.COORDINATOR_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getMetricsSystem(), getContext(), getHostname(),
        Property.COORDINATOR_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.COORDINATOR_PORTSEARCH, Property.COORDINATOR_MINTHREADS,
        Property.COORDINATOR_MINTHREADS_TIMEOUT, Property.COORDINATOR_THREADCHECK,
        maxMessageSizeProperty);
    LOG.info("address = {}", sp.address);
    return sp;
  }

  @Override
  public void run() {

    ServerAddress coordinatorAddress = null;
    try {
      coordinatorAddress = startCoordinatorClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the coordinator service", e1);
    }
    final HostAndPort clientAddress = coordinatorAddress.address;

    try {
      if (!getCoordinatorLock(clientAddress)) {
        throw new RuntimeException("Unable to get Coordinator lock.");
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Exception getting Coordinator lock", e);
    }

    // On a re-start of the coordinator it's possible that external compactions are in-progress.
    // Attempt to get the running compactions on the compactors and then resolve which tserver
    // the external compaction came from to re-populate the RUNNING collection.
    LOG.info("Checking for running external compactions");
    tserverSet.scanServers();
    final Set<TServerInstance> tservers = tserverSet.getCurrentServers();
    if (null != tservers && !tservers.isEmpty()) {
      // On re-start contact the running Compactors to try and seed the list of running compactions
      Map<HostAndPort,TExternalCompactionJob> running =
          ExternalCompactionUtil.getCompactionsRunningOnCompactors(getContext());
      if (running.isEmpty()) {
        LOG.info("No compactions running on Compactors.");
      } else {
        LOG.info("Found {} running external compactions", running.size());
        running.forEach((hp, job) -> {
          // Find the tserver that has this compaction id
          boolean matchFound = false;

          // Attempt to find the TServer hosting the tablet based on the metadata table
          // CBUG use #1974 for more efficient metadata reads
          KeyExtent extent = KeyExtent.fromThrift(job.getExtent());
          TabletMetadata tabletMetadata = getContext().getAmple().readTablets().forTablet(extent)
              .fetch(ColumnType.LOCATION, ColumnType.PREV_ROW).build().stream().findFirst()
              .orElse(null);

          if (tabletMetadata != null && tabletMetadata.getExtent().equals(extent)
              && tabletMetadata.getLocation() != null
              && tabletMetadata.getLocation().getType() == LocationType.CURRENT) {

            TServerInstance tsi = tservers.stream()
                .filter(
                    t -> t.getHostAndPort().equals(tabletMetadata.getLocation().getHostAndPort()))
                .findFirst().orElse(null);

            if (null != tsi) {
              TabletClientService.Client client = null;
              try {
                client = getTabletServerConnection(tsi);
                boolean tserverMatch = client.isRunningExternalCompaction(TraceUtil.traceInfo(),
                    getContext().rpcCreds(), job.getExternalCompactionId(), job.getExtent());
                if (tserverMatch) {
                  RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
                      new RunningCompaction(job, ExternalCompactionUtil.getHostPortString(hp),
                          tsi));
                  matchFound = true;
                }
              } catch (TException e) {
                LOG.warn("Failed to notify tserver {}",
                    tabletMetadata.getLocation().getHostAndPort(), e);
              } finally {
                ThriftUtil.returnClient(client);
              }
            }
          }

          // As a fallback, try them all
          if (!matchFound) {
            for (TServerInstance tsi : tservers) {
              TabletClientService.Client client = null;
              try {
                client = getTabletServerConnection(tsi);
                boolean tserverMatch = client.isRunningExternalCompaction(TraceUtil.traceInfo(),
                    getContext().rpcCreds(), job.getExternalCompactionId(), job.getExtent());
                if (tserverMatch) {
                  RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
                      new RunningCompaction(job, ExternalCompactionUtil.getHostPortString(hp),
                          tsi));
                  matchFound = true;
                }
              } catch (TException e) {
                LOG.error(
                    "Error from tserver {} while trying to check if external compaction is running, trying next tserver",
                    ExternalCompactionUtil.getHostPortString(tsi.getHostAndPort()), e);
              } finally {
                ThriftUtil.returnClient(client);
              }
            }
          }

          if (!matchFound) {
            // There is an external compaction running on a Compactor, but we can't resolve it to a
            // TServer?
            // CBUG: Cancel the compaction?
          }
        });
      }
    } else {
      LOG.info("No running tablet servers found, continuing startup");
    }
    tservers.clear();

    tserverSet.startListeningForTabletServerChanges();
    new DeadCompactionDetector(getContext(), compactionFinalizer).start();

    while (true) {
      long start = System.currentTimeMillis();
      tserverSet.getCurrentServers().forEach(tsi -> {
        try {
          TabletClientService.Client client = null;
          try {
            LOG.debug("Contacting tablet server {} to get external compaction summaries",
                tsi.getHostPort());
            client = getTabletServerConnection(tsi);
            List<TCompactionQueueSummary> summaries =
                client.getCompactionQueueInfo(TraceUtil.traceInfo(), getContext().rpcCreds());
            summaries.forEach(summary -> {
              QueueAndPriority qp =
                  QueueAndPriority.get(summary.getQueue().intern(), summary.getPriority());
              synchronized (qp) {
                QUEUES.computeIfAbsent(qp.getQueue(), k -> new TreeMap<>())
                    .computeIfAbsent(qp.getPriority(), k -> new LinkedHashSet<>()).add(tsi);
                INDEX.computeIfAbsent(tsi, k -> new HashSet<>()).add(qp);
              }
            });
          } finally {
            ThriftUtil.returnClient(client);
          }
        } catch (TException e) {
          LOG.warn("Error getting external compaction summaries from tablet server: {}",
              tsi.getHostAndPort(), e);
        }
      });
      long duration = (System.currentTimeMillis() - start);
      if (TSERVER_CHECK_INTERVAL - duration > 0) {
        UtilWaitThread.sleep(TSERVER_CHECK_INTERVAL - duration);
      }
    }

  }

  /**
   * Callback for the LiveTServerSet object to update current set of tablet servers, including ones
   * that were deleted and added
   *
   * @param current
   *          current set of live tservers
   * @param deleted
   *          set of tservers that were removed from current since last update
   * @param added
   *          set of tservers that were added to current since last update
   */
  @Override
  public void update(LiveTServerSet current, Set<TServerInstance> deleted,
      Set<TServerInstance> added) {

    // run() will iterate over the current and added tservers and add them to the internal
    // data structures. For tservers that are deleted, we need to remove them from QUEUES
    // and INDEX and cancel and RUNNING compactions as we currently don't have a way
    // to notify a tabletserver that a compaction has completed when the tablet is re-hosted.
    deleted.forEach(tsi -> {
      // Find any running compactions for the tserver
      final List<ExternalCompactionId> toCancel = new ArrayList<>();
      RUNNING.forEach((k, v) -> {
        if (v.getTserver().equals(tsi)) {
          toCancel.add(k);
        }
      });
      // Remove the tserver from the QUEUES and INDEX
      INDEX.get(tsi).forEach(qp -> {
        TreeMap<Long,LinkedHashSet<TServerInstance>> m = QUEUES.get(qp.getQueue());
        if (null != m) {
          LinkedHashSet<TServerInstance> tservers = m.get(qp.getPriority());
          if (null != tservers) {
            synchronized (qp) {
              tservers.remove(tsi);
              INDEX.remove(tsi);
            }
          }
        }
      });
      // Cancel running compactions
      toCancel.forEach(id -> {
        try {
          cancelCompaction(id.canonical());
        } catch (TException e) {
          LOG.error("Error cancelling running compaction {} due to tserver {} removal.", id, tsi,
              e);
        }
      });
    });
  }

  /**
   * Return the next compaction job from the queue to a Compactor
   *
   * @param queueName
   *          queue
   * @param compactorAddress
   *          compactor address
   * @return compaction job
   */
  @Override
  public TExternalCompactionJob getCompactionJob(TInfo tinfo, TCredentials credentials,
      String queueName, String compactorAddress, String externalCompactionId) throws TException {

    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    LOG.debug("getCompactionJob " + queueName + " " + compactorAddress);
    final String queue = queueName.intern();
    TExternalCompactionJob result = null;
    final TreeMap<Long,LinkedHashSet<TServerInstance>> m = QUEUES.get(queue);
    if (null != m && !m.isEmpty()) {
      while (result == null) {

        // m could become empty if we have contacted all tservers in this queue and
        // there are no compactions
        if (m.isEmpty()) {
          LOG.debug("No tservers found for queue {}, returning empty job to compactor {}", queue,
              compactorAddress);
          result = new TExternalCompactionJob();
          break;
        }

        // Get the first TServerInstance from the highest priority queue
        final Entry<Long,LinkedHashSet<TServerInstance>> entry = m.firstEntry();
        final Long priority = entry.getKey();
        final LinkedHashSet<TServerInstance> tservers = entry.getValue();
        final QueueAndPriority qp = QueueAndPriority.get(queue, priority);

        if (null == tservers || tservers.isEmpty()) {
          // Clean up the map entry when no tservers for this queue and priority
          m.remove(entry.getKey(), entry.getValue());
          continue;
        } else {
          synchronized (qp) {
            final TServerInstance tserver = tservers.iterator().next();
            LOG.debug("Found tserver {} with priority {} for queue {}", tserver.getHostAndPort(),
                priority, queue);
            // Remove the tserver from the list, we are going to run a compaction on this server
            tservers.remove(tserver);
            if (tservers.isEmpty()) {
              // Clean up the map entry when no tservers remaining for this queue and priority
              // CBUG This may be redundant as cleanup happens in the 'if' clause above
              m.remove(entry.getKey(), entry.getValue());
            }
            final HashSet<QueueAndPriority> queues = INDEX.get(tserver);
            queues.remove(QueueAndPriority.get(queue, priority));
            if (queues.isEmpty()) {
              // Remove the tserver from the index
              INDEX.remove(tserver);
            }
            LOG.debug("Getting compaction for queue {} from tserver {}", queue,
                tserver.getHostAndPort());
            // Get a compaction from the tserver
            TabletClientService.Client client = null;
            try {
              client = getTabletServerConnection(tserver);
              TExternalCompactionJob job = client.reserveCompactionJob(TraceUtil.traceInfo(),
                  getContext().rpcCreds(), queue, priority, compactorAddress, externalCompactionId);
              if (null == job.getExternalCompactionId()) {
                LOG.debug("No compactions found for queue {} on tserver {}, trying next tserver",
                    queue, tserver.getHostAndPort(), compactorAddress);
                continue;
              }
              RUNNING.put(ExternalCompactionId.of(job.getExternalCompactionId()),
                  new RunningCompaction(job, compactorAddress, tserver));
              LOG.debug("Returning external job {} to {}", job.externalCompactionId,
                  compactorAddress);
              result = job;
              break;
            } catch (TException e) {
              LOG.error(
                  "Error from tserver {} while trying to reserve compaction, trying next tserver",
                  ExternalCompactionUtil.getHostPortString(tserver.getHostAndPort()), e);
            } finally {
              ThriftUtil.returnClient(client);
            }
          }
        }
      }
    } else {
      LOG.debug("No tservers found for queue {}, returning empty job to compactor {}", queue,
          compactorAddress);
      result = new TExternalCompactionJob();
    }
    return result;

  }

  /**
   * Return the Thrift client for the TServer
   *
   * @param tserver
   *          tserver instance
   * @return thrift client
   * @throws TTransportException
   */
  protected TabletClientService.Client getTabletServerConnection(TServerInstance tserver)
      throws TTransportException {
    TServerConnection connection = tserverSet.getConnection(tserver);
    TTransport transport =
        ThriftTransportPool.getInstance().getTransport(connection.getAddress(), 0, getContext());
    return ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
  }

  /**
   * Return the Thrift client for the Compactor
   *
   * @param compactorAddress
   *          compactor address
   * @return thrift client
   * @throws TTransportException
   */
  protected Compactor.Client getCompactorConnection(HostAndPort compactorAddress)
      throws TTransportException {
    TTransport transport =
        ThriftTransportPool.getInstance().getTransport(compactorAddress, 0, getContext());
    return ThriftUtil.createClient(new Compactor.Client.Factory(), transport);
  }

  /**
   * Called by the TabletServer to cancel the running compaction.
   */
  @Override
  public void cancelCompaction(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    cancelCompaction(externalCompactionId);
  }

  private void cancelCompaction(String externalCompactionId) throws TException {
    LOG.info("Compaction cancel requested, id: {}", externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ExternalCompactionId.of(externalCompactionId));
    if (null == rc) {
      return;
    }
    if (!rc.isCompleted()) {
      final HostAndPort compactor = HostAndPort.fromString(rc.getCompactorAddress());
      RetryableThriftCall<String> cancelThriftCall = new RetryableThriftCall<>(1000,
          RetryableThriftCall.MAX_WAIT_TIME, 0, new RetryableThriftFunction<String>() {
            @Override
            public String execute() throws TException {
              Compactor.Client compactorConnection = null;
              try {
                compactorConnection = getCompactorConnection(compactor);
                compactorConnection.cancel(TraceUtil.traceInfo(), getContext().rpcCreds(),
                    rc.getJob().getExternalCompactionId());
                return "";
              } catch (TException e) {
                throw e;
              } finally {
                ThriftUtil.returnClient(compactorConnection);
              }
            }
          });
      try {
        cancelThriftCall.run();
      } catch (RetriesExceededException e) {
        LOG.error("Unable to contact Compactor {} to cancel running compaction {}",
            rc.getCompactorAddress(), rc.getJob(), e);
      }
    }
  }

  /**
   * TServer calls getCompactionStatus to get information about the compaction
   *
   * @param externalCompactionId
   *          id
   * @return compaction stats or null if not running
   */
  @Override
  public List<Status> getCompactionStatus(TInfo tinfo, TCredentials credentials,
      String externalCompactionId) throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    final List<Status> status = new ArrayList<>();
    final RunningCompaction rc = RUNNING.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.getUpdates().forEach((k, v) -> {
        status.add(new Status(v.getTimestamp(), rc.getJob().getExternalCompactionId(),
            rc.getCompactorAddress(), v.getState(), v.getMessage()));
      });
    }
    return status;
  }

  /**
   * Compactor calls compactionCompleted passing in the CompactionStats
   *
   * @param job
   *          compaction job
   * @param stats
   *          compaction stats
   */
  @Override
  public void compactionCompleted(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent textent, CompactionStats stats) throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction completed, id: {}, stats: {}", externalCompactionId, stats);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ecid);
    if (null != rc) {
      // CBUG: Should we remove rc from RUNNING here and remove the isCompactionCompleted method?
      rc.setStats(stats);
      rc.setCompleted();
      compactionFinalizer.commitCompaction(ecid, KeyExtent.fromThrift(textent), stats.fileSize,
          stats.entriesWritten);
    } else {
      LOG.error(
          "Compaction completed called by Compactor for {}, but no running compaction for that id.",
          externalCompactionId);
      throw new UnknownCompactionIdException();
    }
  }

  @Override
  public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
      TKeyExtent extent) throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction failed, id: {}", externalCompactionId);
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ecid);
    if (null != rc) {
      // CBUG: Should we remove rc from RUNNING here and remove the isCompactionCompleted method?
      rc.setCompleted();
      compactionFinalizer.failCompactions(Collections
          .singleton(new Pair<ExternalCompactionId,KeyExtent>(ecid, KeyExtent.fromThrift(extent))));
    } else {
      LOG.error(
          "Compaction failed called by Compactor for {}, but no running compaction for that id.",
          externalCompactionId);
      throw new UnknownCompactionIdException();
    }
  }

  /**
   * Called by TabletServer to check if an external compaction has been completed.
   *
   *
   * @param externalCompactionId
   * @return CompactionStats
   * @throws UnknownCompactionIdException
   *           if compaction is not running
   */
  public CompactionStats isCompactionCompleted(String externalCompactionId) throws TException {
    final var ecid = ExternalCompactionId.of(externalCompactionId);
    final RunningCompaction rc = RUNNING.get(ecid);
    if (null != rc && rc.isCompleted()) {
      // CBUG: I don't think this method is ever called, so
      // the running compaction is not removed from RUNNING
      RUNNING.remove(ecid, rc);
      return rc.getStats();
    } else if (rc == null) {
      LOG.error(
          "isCompactionCompleted called by TServer for {}, but no running compaction for that id.",
          externalCompactionId);
      throw new UnknownCompactionIdException();
    } else {
      LOG.debug("isCompactionCompleted called by TServer for {}, but compaction is not complete.",
          externalCompactionId);
      // Return empty stats as a marker that it's not done.
      return new CompactionStats();
    }
  }

  /**
   * Compactor calls to update the status of the assigned compaction
   *
   * @param job
   *          compaction job
   * @param state
   *          compaction state
   * @param message
   *          informational message
   * @param timestamp
   *          timestamp of the message
   */
  @Override
  public void updateCompactionStatus(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, CompactionState state, String message, long timestamp)
      throws TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }
    LOG.info("Compaction status update, id: {}, timestamp: {}, state: {}, message: {}",
        externalCompactionId, timestamp, state, message);
    final RunningCompaction rc = RUNNING.get(ExternalCompactionId.of(externalCompactionId));
    if (null != rc) {
      rc.addUpdate(timestamp, message, state);
    } else {
      throw new UnknownCompactionIdException();
    }
  }

  public static void main(String[] args) throws Exception {
    try (CompactionCoordinator compactor = new CompactionCoordinator(new ServerOpts(), args)) {
      compactor.runServer();
    }
  }

}
