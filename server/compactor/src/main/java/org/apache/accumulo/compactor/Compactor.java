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
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinator;
import org.apache.accumulo.core.compaction.thrift.CompactionState;
import org.apache.accumulo.core.compaction.thrift.Compactor.Iface;
import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.CompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
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
import org.apache.accumulo.server.compaction.Compactor.CompactionEnv;
import org.apache.accumulo.server.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.compaction.RetryableThriftCall;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.compaction.RetryableThriftFunction;
import org.apache.accumulo.server.conf.TableConfiguration;
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

  public static final String COMPACTOR_SERVICE = "COMPACTOR_SVC";

  private static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  private static final CompactionJobHolder jobHolder = new CompactionJobHolder();

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  private final UUID compactorId = UUID.randomUUID();
  private final AccumuloConfiguration aconf;
  private final String queueName;
  private final AtomicReference<CompactionCoordinator.Client> coordinatorClient =
      new AtomicReference<>();

  private ZooLock compactorLock;
  private ServerAddress compactorAddress = null;

  Compactor(CompactorServerOpts opts, String[] args) {
    super("compactor", opts, args);
    queueName = opts.getQueueName();
    ServerContext context = super.getContext();
    context.setupCrypto();

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
  protected void announceExistence(HostAndPort clientAddress)
      throws KeeperException, InterruptedException {

    String hostPort = ExternalCompactionUtil.getHostPortString(clientAddress);

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
   * Start this Compactors thrift service to handle incoming client requests
   *
   * @return address of this compactor client service
   * @throws UnknownHostException
   */
  protected ServerAddress startCompactorClientService() throws UnknownHostException {
    Iface rpcProxy = TraceUtil.wrapService(this);
    final org.apache.accumulo.core.compaction.thrift.Compactor.Processor<Iface> processor;
    if (getContext().getThriftServerType() == ThriftServerType.SASL) {
      Iface tcredProxy =
          TCredentialsUpdatingWrapper.service(rpcProxy, getClass(), getConfiguration());
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
   * Called by a CompactionCoordinator to cancel the currently running compaction
   *
   * @param externalCompactionId
   *          compaction id
   * @throws UnknownCompactionIdException
   *           if the externalCompactionId does not match the currently executing compaction
   */
  @Override
  public void cancel(String externalCompactionId) throws TException {
    synchronized (jobHolder) {
      if (jobHolder.isSet()
          && jobHolder.getJob().getExternalCompactionId().equals(externalCompactionId)) {
        LOG.info("Cancel requested for compaction job {}", externalCompactionId);
        jobHolder.cancel();
        jobHolder.getThread().interrupt();
      } else {
        throw new UnknownCompactionIdException();
      }
    }
  }

  /**
   * Send an update to the CompactionCoordinator for this job
   *
   * @param job
   *          compactionJob
   * @param state
   *          updated state
   * @param message
   *          updated message
   * @throws RetriesExceededException
   */
  protected void updateCompactionState(TExternalCompactionJob job, CompactionState state,
      String message) throws RetriesExceededException {
    // CBUG the return type was changed from Void to String just to make this work. When type was
    // Void and returned null, it would retry forever. Could specialize RetryableThriftCall for case
    // w/ not return type.
    RetryableThriftCall<String> thriftCall = new RetryableThriftCall<>(1000,
        RetryableThriftCall.MAX_WAIT_TIME, 25, new RetryableThriftFunction<String>() {
          @Override
          public String execute() throws TException {
            try {
              coordinatorClient.compareAndSet(null, getCoordinatorClient());
              coordinatorClient.get().updateCompactionStatus(job.getExternalCompactionId(), state,
                  message, System.currentTimeMillis());
              return "";
            } catch (TException e) {
              ThriftUtil.returnClient(coordinatorClient.getAndSet(null));
              throw e;
            }
          }
        });
    thriftCall.run();
  }

  /**
   * Update the CompactionCoordinator with the stats from the completed job
   *
   * @param job
   *          current compaction job
   * @param stats
   *          compaction stats
   * @throws RetriesExceededException
   */
  protected void updateCompactionCompleted(TExternalCompactionJob job, CompactionStats stats)
      throws RetriesExceededException {
    RetryableThriftCall<Void> thriftCall = new RetryableThriftCall<>(1000,
        RetryableThriftCall.MAX_WAIT_TIME, 25, new RetryableThriftFunction<Void>() {
          @Override
          public Void execute() throws TException {
            try {
              coordinatorClient.compareAndSet(null, getCoordinatorClient());
              coordinatorClient.get().compactionCompleted(job.getExternalCompactionId(), stats);
              return null;
            } catch (TException e) {
              ThriftUtil.returnClient(coordinatorClient.getAndSet(null));
              throw e;
            }
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
   * @throws RetriesExceededException
   */
  protected TExternalCompactionJob getNextJob() throws RetriesExceededException {
    RetryableThriftCall<TExternalCompactionJob> nextJobThriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 0,
            new RetryableThriftFunction<TExternalCompactionJob>() {
              @Override
              public TExternalCompactionJob execute() throws TException {
                try {
                  coordinatorClient.compareAndSet(null, getCoordinatorClient());
                  return coordinatorClient.get().getCompactionJob(queueName,
                      ExternalCompactionUtil.getHostPortString(compactorAddress.getAddress()));
                } catch (TException e) {
                  ThriftUtil.returnClient(coordinatorClient.getAndSet(null));
                  throw e;
                }
              }
            });
    return nextJobThriftCall.run();
  }

  /**
   * Get the client to the CompactionCoordinator
   *
   * @return compaction coordinator client
   * @throws TTransportException
   *           when unable to get client
   */
  protected CompactionCoordinator.Client getCoordinatorClient() throws TTransportException {
    HostAndPort coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(getContext());
    if (null == coordinatorHost) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    LOG.info("CompactionCoordinator address is: {}", coordinatorHost);
    return ThriftUtil.getClient(new CompactionCoordinator.Client.Factory(), coordinatorHost,
        getContext());
  }

  /**
   * Create compaction runnable
   *
   * @param job
   *          compaction job
   * @param totalInputEntries
   *          object to capture total entries
   * @param started
   *          started latch
   * @param stopped
   *          stopped latch
   * @param err
   *          reference to error
   * @return Runnable compaction job
   */
  protected Runnable createCompactionJob(final TExternalCompactionJob job,
      final LongAdder totalInputEntries, final CountDownLatch started, final CountDownLatch stopped,
      final AtomicReference<Throwable> err) {

    return new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting up compaction runnable for job: {}", job);
          updateCompactionState(job, CompactionState.STARTED, "Compaction started");

          final TableId tableId = TableId.of(new String(job.getExtent().getTable(), UTF_8));
          final TableConfiguration tConfig = getContext().getTableConfiguration(tableId);
          final TabletFile outputFile = new TabletFile(new Path(job.getOutputFile()));
          final CompactionEnv cenv = new CompactionEnvironment(getContext(), jobHolder);

          final Map<StoredTabletFile,DataFileValue> files = new TreeMap<>();
          job.getFiles().forEach(f -> {
            files.put(new StoredTabletFile(f.getMetadataFileEntry()),
                new DataFileValue(f.getSize(), f.getEntries(), f.getTimestamp()));
            totalInputEntries.add(f.getEntries());
          });

          final List<IteratorSetting> iters = new ArrayList<>();
          job.getIteratorSettings().getIterators()
              .forEach(tis -> iters.add(SystemIteratorUtil.toIteratorSetting(tis)));

          org.apache.accumulo.server.compaction.Compactor compactor =
              new org.apache.accumulo.server.compaction.Compactor(getContext(),
                  KeyExtent.fromThrift(job.getExtent()), files, outputFile,
                  job.isPropagateDeletes(), cenv, iters, tConfig);

          LOG.info("Starting compactor");
          started.countDown();

          org.apache.accumulo.server.compaction.CompactionStats stat = compactor.call();
          CompactionStats cs = new CompactionStats();
          cs.setEntriesRead(stat.getEntriesRead());
          cs.setEntriesWritten(stat.getEntriesWritten());
          cs.setFileSize(stat.getFileSize());
          jobHolder.setStats(cs);
          LOG.info("Compaction completed successfully {} ", job.getExternalCompactionId());
          // Update state when completed
          updateCompactionState(job, CompactionState.SUCCEEDED,
              "Compaction completed successfully");
        } catch (Exception e) {
          LOG.error("Compaction failed", e);
          err.set(e);
          throw new RuntimeException("Compaction failed", e);
        } finally {
          stopped.countDown();
          // CBUG Any cleanup
        }
      }
    };
  }

  @Override
  public void run() {

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

    LOG.info("Compactor started, waiting for work");
    try {

      final AtomicReference<Throwable> err = new AtomicReference<>();

      while (true) {
        err.set(null);
        jobHolder.reset();

        TExternalCompactionJob job;
        try {
          job = getNextJob();
        } catch (RetriesExceededException e2) {
          LOG.warn("Retries exceeded getting next job. Retrying...");
          continue;
        }
        LOG.info("Received next compaction job: {}", job);

        final LongAdder totalInputEntries = new LongAdder();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch stopped = new CountDownLatch(1);

        Thread compactionThread =
            Threads.createThread("Compaction job for tablet " + job.getExtent().toString(),
                this.createCompactionJob(job, totalInputEntries, started, stopped, err));

        synchronized (jobHolder) {
          jobHolder.set(job, compactionThread);
        }

        compactionThread.start(); // start the compactionThread
        try {
          started.await(); // wait until the compactor is started
          long inputEntries = totalInputEntries.sum();

          // This block of code will update the coordinator with the progress of external
          // compaction every minute
          //
          // CBUG Is 1 minute too often for very large files? Since the update is stored in the
          // RUNNING set, it's going to eat up memory. Should the time be dependent on the size
          // of the input entries?
          while (!stopped.await(1, TimeUnit.MINUTES)) {
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
                try {
                  updateCompactionState(job, CompactionState.IN_PROGRESS, message);
                } catch (RetriesExceededException e) {
                  LOG.warn("Error updating coordinator with compaction progress, error: {}",
                      e.getMessage());
                }
              }
            }
          }
          try {
            compactionThread.join();
            this.updateCompactionCompleted(job, jobHolder.getStats());
          } catch (InterruptedException e) {
            LOG.error(
                "Compactor thread was interrupted waiting for compaction to finish, cancelling job",
                e);
            try {
              cancel(job.getExternalCompactionId());
            } catch (TException e1) {
              LOG.error("Error cancelling compaction.", e1);
            }
          } catch (RetriesExceededException e) {
            LOG.error(
                "Error updating coordinator with compaction completion, cancelling compaction.", e);
            try {
              cancel(job.getExternalCompactionId());
            } catch (TException e1) {
              LOG.error("Error cancelling compaction.", e1);
            }
          }

        } catch (InterruptedException e1) {
          LOG.error(
              "Compactor thread was interrupted waiting for compaction to start, cancelling job",
              e1);
          try {
            cancel(job.getExternalCompactionId());
          } catch (TException e2) {
            LOG.error("Error cancelling compaction.", e2);
          }
        }

        if (compactionThread.isInterrupted()) {
          LOG.warn("Compaction thread was interrupted, sending CANCELLED state");
          try {
            updateCompactionState(job, CompactionState.CANCELLED, "Compaction cancelled");
          } catch (RetriesExceededException e) {
            LOG.error("Error updating coordinator with compaction cancellation.", e);
          }
        }

        Throwable thrown = err.get();
        if (thrown != null) {
          try {
            updateCompactionState(job, CompactionState.FAILED,
                "Compaction failed due to: " + thrown.getMessage());
          } catch (RetriesExceededException e) {
            LOG.error("Error updating coordinator with compaction failure.", e);
          }
        }
      }

    } catch (Exception e) {
      LOG.error("Unhandled error occurred in Compactor", e);
    } finally {
      // close connection to coordinator
      if (null != coordinatorClient.get()) {
        ThriftUtil.returnClient(coordinatorClient.get());
      }

      // Shutdown local thrift server
      LOG.info("Stopping Thrift Servers");
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
