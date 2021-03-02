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
package org.apache.accumulo.compactor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinator;
import org.apache.accumulo.core.compaction.thrift.CompactionJob;
import org.apache.accumulo.core.compaction.thrift.CompactionState;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.CompactionReason;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.ratelimit.SharedRateLimiterFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.compaction.CompactionInfo;
import org.apache.accumulo.server.compaction.CompactionStats;
import org.apache.accumulo.server.compaction.Compactor.CompactionEnv;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class Compactor extends AbstractServer
    implements org.apache.accumulo.core.compaction.thrift.Compactor.Iface {

  private static class CompactorServerOpts extends ServerOpts {
    @Parameter(required = true, names = {"-q", "--queue"}, description = "compaction queue name")
    private String queueName = null;

    public String getQueueName() {
      return queueName;
    }
  }

  /**
   * Object used to hold information about the current compaction
   */
  private static class CompactionJobHolder {
    private CompactionJob job;
    private Thread compactionThread;
    private volatile Boolean cancelled = Boolean.FALSE;
    private CompactionStats stats = null;

    public CompactionJobHolder() {}

    public void reset() {
      job = null;
      compactionThread = null;
      cancelled = Boolean.FALSE;
      stats = null;
    }

    public CompactionJob getJob() {
      return job;
    }

    public Thread getThread() {
      return compactionThread;
    }

    public CompactionStats getStats() {
      return stats;
    }

    public void setStats(CompactionStats stats) {
      this.stats = stats;
    }

    public void cancel() {
      cancelled = Boolean.TRUE;
    }

    public boolean isCancelled() {
      return cancelled;
    }

    public boolean isSet() {
      return (null != this.job);
    }

    public void set(CompactionJob job, Thread compactionThread) {
      Objects.requireNonNull(job, "CompactionJob is null");
      Objects.requireNonNull(compactionThread, "Compaction thread is null");
      this.job = job;
      this.compactionThread = compactionThread;
    }

  }

  /**
   * Utility for returning the address in the form host:port
   *
   * @return host and port for Compactor client connections
   */
  private static String getHostPortString(HostAndPort address) {
    if (address == null) {
      return null;
    }
    return address.getHost() + ":" + address.getPort();
  }

  public static final String COMPACTOR_SERVICE = "COMPACTOR_SVC";

  private static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  private static final long MAX_WAIT_TIME = 60000;

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  private final UUID compactorId = UUID.randomUUID();
  private final AccumuloConfiguration aconf;
  private final String queueName;
  private final CompactionJobHolder jobHolder;
  private ZooLock compactorLock;

  Compactor(CompactorServerOpts opts, String[] args) {
    super("compactor", opts, args);
    queueName = opts.getQueueName();
    ServerContext context = super.getContext();
    context.setupCrypto();

    this.jobHolder = new CompactionJobHolder();
    aconf = getConfiguration();
    ThreadPools.createGeneralScheduledExecutorService(aconf).scheduleWithFixedDelay(
        () -> gcLogger.logGCInfo(getConfiguration()), 0, TIME_BETWEEN_GC_CHECKS,
        TimeUnit.MILLISECONDS);
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + context.getInstanceID());
  }

  /**
   * Set up nodes and locks in ZooKeeper for this Compactor
   *
   * @param clientAddress
   *          address of this Compactor
   *
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void announceExistence(HostAndPort clientAddress)
      throws KeeperException, InterruptedException {

    String hostPort = getHostPortString(clientAddress);

    ZooReaderWriter zoo = getContext().getZooReaderWriter();
    String compactorQueuePath =
        getContext().getZooKeeperRoot() + Constants.ZCOMPACTORS + "/" + this.queueName;
    String zPath = compactorQueuePath + "/" + hostPort;

    try {
      zoo.mkdirs(compactorQueuePath);
      zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NOAUTH) {
        LOG.error("Failed to write to ZooKeeper. Ensure that"
            + " accumulo.properties, specifically instance.secret, is consistent.");
      }
      throw e;
    }

    compactorLock = new ZooLock(getContext().getSiteConfiguration(), zPath, compactorId);
    LockWatcher lw = new LockWatcher() {
      @Override
      public void lostLock(final LockLossReason reason) {
        Halt.halt(1, () -> {
          LOG.error("Compactor lost lock (reason = {}), exiting.", reason);
          gcLogger.logGCInfo(getConfiguration());
        });
      }

      @Override
      public void unableToMonitorLockNode(final Exception e) {
        Halt.halt(1, () -> LOG.error("Lost ability to monitor Compactor lock, exiting.", e));
      }
    };

    try {
      byte[] lockContent = (hostPort + "=" + COMPACTOR_SERVICE).getBytes(UTF_8);
      for (int i = 0; i < 25; i++) {
        zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.SKIP);

        if (compactorLock.tryLock(lw, lockContent)) {
          LOG.debug("Obtained Compactor lock {}", compactorLock.getLockPath());
          return;
        }
        LOG.info("Waiting for Compactor lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      LOG.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      LOG.info("Could not obtain tablet server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the address of the CompactionCoordinator
   *
   * @return address of Coordinator
   */
  private HostAndPort getCoordinatorAddress() {
    try {
      // TODO: Get the coordinator location from ZooKeeper
      List<String> locations = null;
      if (locations.isEmpty()) {
        return null;
      }
      return HostAndPort.fromString(locations.get(0));
    } catch (Exception e) {
      LOG.warn("Failed to obtain manager host " + e);
    }

    return null;
  }

  /**
   * Start this Compactors thrift service to handle incoming client requests
   *
   * @return address of this compactor client service
   * @throws UnknownHostException
   */
  private ServerAddress startCompactorClientService() throws UnknownHostException {
    Compactor rpcProxy = TraceUtil.wrapService(this);
    final org.apache.accumulo.core.compaction.thrift.Compactor.Processor<Compactor> processor;
    if (getContext().getThriftServerType() == ThriftServerType.SASL) {
      Compactor tcredProxy =
          TCredentialsUpdatingWrapper.service(rpcProxy, Compactor.class, getConfiguration());
      processor = new org.apache.accumulo.core.compaction.thrift.Compactor.Processor<>(tcredProxy);
    } else {
      processor = new org.apache.accumulo.core.compaction.thrift.Compactor.Processor<>(rpcProxy);
    }
    Property maxMessageSizeProperty = (aconf.get(Property.COMPACTOR_MAX_MESSAGE_SIZE) != null
        ? Property.COMPACTOR_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getMetricsSystem(), getContext(), getHostname(),
        Property.COMPACTOR_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.COMPACTOR_PORTSEARCH, Property.COMPACTOR_MINTHREADS,
        Property.COMPACTOR_MINTHREADS_TIMEOUT, Property.COMPACTOR_THREADCHECK,
        maxMessageSizeProperty);
    LOG.info("address = {}", sp.address);
    return sp;
  }

  /**
   * Called by a thrift client to cancel the currently running compaction if it matches the supplied
   * job
   *
   * @param compactionJob
   *          job
   */
  @Override
  public void cancel(CompactionJob compactionJob) {
    synchronized (jobHolder) {
      if (jobHolder.isSet() && jobHolder.getJob().equals(compactionJob)) {
        LOG.info("Cancel requested for compaction job {}", compactionJob);
        jobHolder.cancel();
        jobHolder.getThread().interrupt();
      }
    }
  }

  /**
   * Send an update to the coordinator for this job
   * 
   * @param coordinatorClient
   *          address of the CompactionCoordinator
   * @param job
   *          compactionJob
   * @param state
   *          updated state
   * @param message
   *          updated message
   */
  private void updateCompactionState(CompactionCoordinator.Client coordinatorClient,
      CompactionJob job, CompactionState state, String message) {
    RetryableThriftCall<Void> thriftCall =
        new RetryableThriftCall<>(1000, MAX_WAIT_TIME, 25, new RetryableThriftFunction<Void>() {
          @Override
          public Void execute() throws TException {
            coordinatorClient.updateCompactionState(job, state, message,
                System.currentTimeMillis());
            return null;
          }
        });
    thriftCall.run();
  }

  /**
   * Get the next job to run
   *
   * @param coordinatorClient
   *          address of the CompactionCoordinator
   * @param compactorAddress
   *          address of this Compactor
   * @return CompactionJob
   */
  private CompactionJob getNextJob(CompactionCoordinator.Client coordinatorClient,
      String compactorAddress) {
    RetryableThriftCall<CompactionJob> nextJobThriftCall = new RetryableThriftCall<>(1000,
        MAX_WAIT_TIME, 0, new RetryableThriftFunction<CompactionJob>() {
          @Override
          public CompactionJob execute() throws TException {
            return coordinatorClient.getCompactionJob(queueName, compactorAddress);
          }
        });
    return nextJobThriftCall.run();
  }

  @Override
  public void run() {

    ServerAddress compactorAddress = null;
    try {
      compactorAddress = startCompactorClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the compactor client service", e1);
    }
    final HostAndPort clientAddress = compactorAddress.address;

    try {
      announceExistence(clientAddress);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Erroring registering in ZooKeeper", e);
    }

    HostAndPort coordinatorHost = getCoordinatorAddress();
    if (null == coordinatorHost) {
      throw new RuntimeException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    LOG.info("CompactionCoordinator address is: {}", coordinatorHost);
    CompactionCoordinator.Client coordinatorClient;
    try {
      coordinatorClient = ThriftUtil.getClient(new CompactionCoordinator.Client.Factory(),
          coordinatorHost, getContext());
    } catch (TTransportException e2) {
      throw new RuntimeException("Erroring connecting to CompactionCoordinator", e2);
    }

    LOG.info("Compactor started, waiting for work");
    try {

      final AtomicReference<Throwable> err = new AtomicReference<>();

      while (true) {
        err.set(null);
        jobHolder.reset();
        final CompactionJob job = getNextJob(coordinatorClient, getHostPortString(clientAddress));

        LOG.info("Received next compaction job: {}", job);

        final LongAdder totalInputSize = new LongAdder();
        final LongAdder totalInputEntries = new LongAdder();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch stopped = new CountDownLatch(1);

        Thread compactionThread = Threads.createThread(
            "Compaction job for tablet " + job.getExtent().toString(), new Runnable() {
              @Override
              public void run() {
                try {
                  LOG.info("Setting up to run compactor");
                  updateCompactionState(coordinatorClient, job, CompactionState.STARTED,
                      "Compaction started");

                  final TableId tableId = TableId.of(new String(job.getExtent().getTable(), UTF_8));
                  final TableConfiguration tConfig = getContext().getTableConfiguration(tableId);

                  final Map<StoredTabletFile,DataFileValue> files = new TreeMap<>();
                  job.getFiles().forEach(f -> {
                    files.put(new StoredTabletFile(f.getMetadataFileEntry()),
                        new DataFileValue(f.getSize(), f.getEntries(), f.getTimestamp()));
                    totalInputSize.add(f.getSize());
                    totalInputEntries.add(f.getEntries());
                  });

                  final TabletFile outputFile = new TabletFile(new Path(job.getOutputFile()));

                  final CompactionEnv cenv = new CompactionEnv() {
                    @Override
                    public boolean isCompactionEnabled() {
                      return !jobHolder.isCancelled();
                    }

                    @Override
                    public IteratorScope getIteratorScope() {
                      return IteratorScope.majc;
                    }

                    @Override
                    public RateLimiter getReadLimiter() {
                      return SharedRateLimiterFactory.getInstance(getContext().getConfiguration())
                          .create("read_rate_limiter", () -> job.getReadRate());
                    }

                    @Override
                    public RateLimiter getWriteLimiter() {
                      return SharedRateLimiterFactory.getInstance(getContext().getConfiguration())
                          .create("write_rate_limiter", () -> job.getWriteRate());
                    }

                    @Override
                    public SystemIteratorEnvironment createIteratorEnv(ServerContext context,
                        AccumuloConfiguration acuTableConf, TableId tableId) {
                      return new TabletIteratorEnvironment(getContext(), IteratorScope.majc,
                          !job.isPropagateDeletes(), acuTableConf, tableId,
                          CompactionKind.valueOf(job.getKind().name()));
                    }

                    @Override
                    public SortedKeyValueIterator<Key,Value> getMinCIterator() {
                      throw new UnsupportedOperationException();
                    }

                    @Override
                    public CompactionReason getReason() {
                      switch (job.getKind()) {
                        case USER:
                          return CompactionReason.USER;
                        case CHOP:
                          return CompactionReason.CHOP;
                        case SELECTOR:
                        case SYSTEM:
                        default:
                          return CompactionReason.SYSTEM;
                      }
                    }
                  };

                  final List<IteratorSetting> iters = new ArrayList<>();
                  job.getIteratorSettings().getIterators()
                      .forEach(tis -> iters.add(SystemIteratorUtil.toIteratorSetting(tis)));

                  org.apache.accumulo.server.compaction.Compactor compactor =
                      new org.apache.accumulo.server.compaction.Compactor(getContext(),
                          KeyExtent.fromThrift(job.getExtent()), files, outputFile,
                          job.isPropagateDeletes(), cenv, iters, tConfig);

                  LOG.info("Starting compactor");
                  started.countDown();
                  jobHolder.setStats(compactor.call());

                  LOG.info("Compaction completed successfully");
                  // Update state when completed
                  updateCompactionState(coordinatorClient, job, CompactionState.SUCCEEDED,
                      "Compaction completed successfully");
                } catch (Exception e) {
                  LOG.error("Compaction failed", e);
                  err.set(e);
                  throw new RuntimeException("Compaction failed", e);
                } finally {
                  stopped.countDown();
                  // TODO: Any cleanup
                }
              }
            });

        synchronized (jobHolder) {
          jobHolder.set(job, compactionThread);
        }

        compactionThread.start(); // start the compactionThread
        try {
          started.await(); // wait until the compactor is started
          long inputEntries = totalInputEntries.sum();
          while (stopped.getCount() > 0) {
            List<CompactionInfo> running =
                org.apache.accumulo.server.compaction.Compactor.getRunningCompactions();
            if (!running.isEmpty()) {
              // Compaction has started. There should only be one in the list
              CompactionInfo info = running.get(0);
              if (info != null) {
                String message = String.format(
                    "Compaction in progress, read %d of %d input entries (%f %), written %d entries",
                    info.getEntriesRead(), inputEntries,
                    (info.getEntriesRead() / inputEntries) * 100, info.getEntriesWritten());
                LOG.info(message);
                updateCompactionState(coordinatorClient, job, CompactionState.IN_PROGRESS, message);
              }
            }
            UtilWaitThread.sleep(MAX_WAIT_TIME);
          }
          try {
            compactionThread.join();
            CompactionStats stats = jobHolder.getStats();
            // TODO: Tell coordinator that we are finished, send stats.

          } catch (InterruptedException e) {
            LOG.error(
                "Compactor thread was interrupted waiting for compaction to finish, cancelling job",
                e);
            cancel(job);
          }

        } catch (InterruptedException e1) {
          LOG.error(
              "Compactor thread was interrupted waiting for compaction to start, cancelling job",
              e1);
          cancel(job);
        }

        if (compactionThread.isInterrupted()) {
          LOG.warn("Compaction thread was interrupted, sending CANCELLED state");
          updateCompactionState(coordinatorClient, job, CompactionState.CANCELLED,
              "Compaction cancelled");
        }

        Throwable thrown = err.get();
        if (thrown != null) {
          updateCompactionState(coordinatorClient, job, CompactionState.FAILED,
              "Compaction failed due to: " + thrown.getMessage());
        }
      }

    } finally {
      // close connection to coordinator
      ThriftUtil.returnClient(coordinatorClient);

      // Shutdown local thrift server
      LOG.debug("Stopping Thrift Servers");
      TServerUtils.stopTServer(compactorAddress.server);

      try {
        LOG.debug("Closing filesystems");
        getContext().getVolumeManager().close();
      } catch (IOException e) {
        LOG.warn("Failed to close filesystem : {}", e.getMessage(), e);
      }

      gcLogger.logGCInfo(getConfiguration());
      LOG.info("stop requested. exiting ... ");
      try {
        compactorLock.unlock();
      } catch (Exception e) {
        LOG.warn("Failed to release compactor lock", e);
      }
    }

  }

  public static void main(String[] args) throws Exception {
    try (Compactor compactor = new Compactor(new CompactorServerOpts(), args)) {
      compactor.runServer();
    }
  }

}
