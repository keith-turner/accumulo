/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.impl;

import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.cloudtrace.instrument.Span;
import org.apache.accumulo.cloudtrace.instrument.Trace;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.RackUtil.ServerSelector;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.UpdateErrors;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;


/*
 * Differences from previous TabletServerBatchWriter
 *   + As background threads finish sending mutations to tablet servers they decrement memory usage
 *   + Once the queue of unprocessed mutations reaches 50% it is always pushed to the background threads, 
 *      even if they are currently processing... new mutations are merged with mutations currently 
 *      processing in the background
 *   + Failed mutations are held for 1000ms and then re-added to the unprocessed queue
 *   + Flush holds adding of new mutations so it does not wait indefinitely
 * 
 * Considerations
 *   + All background threads must catch and note Throwable
 *   + mutations for a single tablet server are only processed by one thread concurrently (if new mutations 
 *      come in for a tablet server while one thread is processing mutations for it, no other thread should 
 *      start processing those mutations)
 *   
 * Memory accounting
 *   + when a mutation enters the system memory is incremented
 *   + when a mutation successfully leaves the system memory is decremented
 * 
 * 
 * 
 */

public class TabletServerBatchWriter {
  
  private static final Logger log = Logger.getLogger(TabletServerBatchWriter.class);
  
  private long totalMemUsed = 0;
  private long maxMem;
  private MutationSet mutations;
  private boolean flushing;
  private boolean closed;
  private MutationWriter writer;
  private FailedMutations failedMutations;
  
  private Instance instance;
  private AuthInfo credentials;
  
  private Violations violations;
  private HashSet<KeyExtent> authorizationFailures;
  private HashSet<String> serverSideErrors;
  private int unknownErrors = 0;
  private boolean somethingFailed = false;
  
  private Timer jtimer;
  
  private long maxLatency;
  
  private long lastProcessingStartTime;
  
  private long totalAdded = 0;
  private AtomicLong totalSent = new AtomicLong(0);
  private AtomicLong totalBinned = new AtomicLong(0);
  private AtomicLong totalBinTime = new AtomicLong(0);
  private AtomicLong totalSendTime = new AtomicLong(0);
  private long startTime = 0;
  private long initialGCTimes;
  private long initialCompileTimes;
  private double initialSystemLoad;
  
  private int tabletServersBatchSum = 0;
  private int tabletBatchSum = 0;
  private int numBatches = 0;
  private int maxTabletBatch = Integer.MIN_VALUE;
  private int minTabletBatch = Integer.MAX_VALUE;
  private int minTabletServersBatch = Integer.MAX_VALUE;
  private int maxTabletServersBatch = Integer.MIN_VALUE;
  
  private Throwable lastUnknownError = null;
  
  public TabletServerBatchWriter(Instance instance, AuthInfo credentials, BatchWriterConfig config) {
    this(instance, credentials, config.getMaxMemory(), config.getMaxLatency(), config.getMaxWriteThreads(), config.getMapping());
  }
  
  public TabletServerBatchWriter(Instance instance, AuthInfo credentials, long maxMemory, long maxLatency, int numSendThreads) {
    this(instance, credentials, maxMemory, maxLatency, numSendThreads, null);
  }
  
  public TabletServerBatchWriter(Instance instance, AuthInfo credentials, long maxMemory, long maxLatency, int numSendThreads, DNSToSwitchMapping mapping) {
    this.instance = instance;
    this.maxMem = maxMemory;
    this.maxLatency = maxLatency <= 0 ? Long.MAX_VALUE : maxLatency;
    this.credentials = credentials;
    mutations = new MutationSet();
    
    violations = new Violations();
    
    authorizationFailures = new HashSet<KeyExtent>();
    serverSideErrors = new HashSet<String>();
    
    lastProcessingStartTime = System.currentTimeMillis();
    
    jtimer = new Timer("BatchWriterLatencyTimer", true);
    
    if (mapping == null)
      writer = new MutationWriter(new TabletServerMutationBinner(), numSendThreads);
    else
      writer = new MutationWriter(new RackMutationBinner(new CachedDNSToSwitchMapping(mapping)), numSendThreads);

    failedMutations = new FailedMutations();
    
    if (this.maxLatency != Long.MAX_VALUE) {
      jtimer.schedule(new TimerTask() {
        public void run() {
          try {
            synchronized (TabletServerBatchWriter.this) {
              if ((System.currentTimeMillis() - lastProcessingStartTime) > TabletServerBatchWriter.this.maxLatency)
                startProcessing();
            }
          } catch (Throwable t) {
            updateUnknownErrors("Max latency task failed " + t.getMessage(), t);
          }
        }
      }, 0, this.maxLatency / 4);
    }
  }

  private synchronized void startProcessing() {
    if (mutations.getMemoryUsed() == 0)
      return;
    lastProcessingStartTime = System.currentTimeMillis();
    writer.addMutations(mutations);
    mutations = new MutationSet();
  }
  
  private synchronized void decrementMemUsed(long amount) {
    totalMemUsed -= amount;
    this.notifyAll();
  }
  
  public synchronized void addMutation(String table, Mutation m) throws MutationsRejectedException {
    
    if (closed)
      throw new IllegalStateException("Closed");
    if (m.size() == 0)
      throw new IllegalArgumentException("Can not add empty mutations");
    
    checkForFailures();
    
    while ((totalMemUsed >= maxMem || flushing) && !somethingFailed) {
      waitRTE();
    }
    
    // do checks again since things could have changed while waiting and not holding lock
    if (closed)
      throw new IllegalStateException("Closed");
    checkForFailures();
    
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
      
      List<GarbageCollectorMXBean> gcmBeans = ManagementFactory.getGarbageCollectorMXBeans();
      for (GarbageCollectorMXBean garbageCollectorMXBean : gcmBeans) {
        initialGCTimes += garbageCollectorMXBean.getCollectionTime();
      }
      
      CompilationMXBean compMxBean = ManagementFactory.getCompilationMXBean();
      if (compMxBean.isCompilationTimeMonitoringSupported()) {
        initialCompileTimes = compMxBean.getTotalCompilationTime();
      }
      
      initialSystemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    }
    
    // create a copy of mutation so that after this method returns the user
    // is free to reuse the mutation object, like calling readFields... this
    // is important for the case where a mutation is passed from map to reduce
    // to batch writer... the map reduce code will keep passing the same mutation
    // object into the reduce method
    m = new Mutation(m);
    
    totalMemUsed += m.estimatedMemoryUsed();
    mutations.addMutation(table, m);
    totalAdded++;
    
    if (mutations.getMemoryUsed() >= maxMem / 2) {
      startProcessing();
      checkForFailures();
    }
  }
  
  public synchronized void addMutation(String table, Iterator<Mutation> iterator) throws MutationsRejectedException {
    while (iterator.hasNext()) {
      addMutation(table, iterator.next());
    }
  }
  
  public synchronized void flush() throws MutationsRejectedException {
    
    if (closed)
      throw new IllegalStateException("Closed");
    
    Span span = Trace.start("flush");
    
    try {
      checkForFailures();
      
      if (flushing) {
        // some other thread is currently flushing, so wait
        while (flushing && !somethingFailed)
          waitRTE();
        
        checkForFailures();
        
        return;
      }
      
      flushing = true;
      
      startProcessing();
      checkForFailures();
      
      while (totalMemUsed > 0 && !somethingFailed) {
        waitRTE();
      }
      
      flushing = false;
      this.notifyAll();
      
      checkForFailures();
    } finally {
      span.stop();
    }
  }
  
  public synchronized void close() throws MutationsRejectedException {
    
    if (closed)
      return;
    
    Span span = Trace.start("close");
    try {
      closed = true;
      
      startProcessing();
      
      while (totalMemUsed > 0 && !somethingFailed) {
        waitRTE();
      }
      
      logStats();
      
      checkForFailures();
    } finally {
      // make a best effort to release these resources
      writer.sendThreadPool.shutdownNow();
      jtimer.cancel();
      span.stop();
    }
  }
  
  private void logStats() {
    long finishTime = System.currentTimeMillis();
    
    long finalGCTimes = 0;
    List<GarbageCollectorMXBean> gcmBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean garbageCollectorMXBean : gcmBeans) {
      finalGCTimes += garbageCollectorMXBean.getCollectionTime();
    }
    
    CompilationMXBean compMxBean = ManagementFactory.getCompilationMXBean();
    long finalCompileTimes = 0;
    if (compMxBean.isCompilationTimeMonitoringSupported()) {
      finalCompileTimes = compMxBean.getTotalCompilationTime();
    }
    
    double averageRate = totalSent.get() / (totalSendTime.get() / 1000.0);
    double overallRate = totalAdded / ((finishTime - startTime) / 1000.0);
    
    double finalSystemLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    
    if (log.isTraceEnabled()) {
      log.trace("");
      log.trace("TABLET SERVER BATCH WRITER STATISTICS");
      log.trace(String.format("Added                : %,10d mutations", totalAdded));
      log.trace(String.format("Sent                 : %,10d mutations", totalSent.get()));
      log.trace(String.format("Resent percentage   : %10.2f%s", (totalSent.get() - totalAdded) / (double) totalAdded * 100.0, "%"));
      log.trace(String.format("Overall time         : %,10.2f secs", (finishTime - startTime) / 1000.0));
      log.trace(String.format("Overall send rate    : %,10.2f mutations/sec", overallRate));
      log.trace(String.format("Send efficiency      : %10.2f%s", overallRate / averageRate * 100.0, "%"));
      log.trace("");
      log.trace("BACKGROUND WRITER PROCESS STATISTICS");
      log.trace(String.format("Total send time      : %,10.2f secs %6.2f%s", totalSendTime.get() / 1000.0, 100.0 * totalSendTime.get()
          / (finishTime - startTime), "%"));
      log.trace(String.format("Average send rate    : %,10.2f mutations/sec", averageRate));
      log.trace(String.format("Total bin time       : %,10.2f secs %6.2f%s", totalBinTime.get() / 1000.0,
          100.0 * totalBinTime.get() / (finishTime - startTime), "%"));
      log.trace(String.format("Average bin rate     : %,10.2f mutations/sec", totalBinned.get() / (totalBinTime.get() / 1000.0)));
      log.trace(String.format("tservers per batch   : %,8.2f avg  %,6d min %,6d max", (tabletServersBatchSum / (double) numBatches), minTabletServersBatch,
          maxTabletServersBatch));
      log.trace(String.format("tablets per batch    : %,8.2f avg  %,6d min %,6d max", (tabletBatchSum / (double) numBatches), minTabletBatch, maxTabletBatch));
      log.trace("");
      log.trace("SYSTEM STATISTICS");
      log.trace(String.format("JVM GC Time          : %,10.2f secs", ((finalGCTimes - initialGCTimes) / 1000.0)));
      if (compMxBean.isCompilationTimeMonitoringSupported()) {
        log.trace(String.format("JVM Compile Time     : %,10.2f secs", ((finalCompileTimes - initialCompileTimes) / 1000.0)));
      }
      log.trace(String.format("System load average : initial=%6.2f final=%6.2f", initialSystemLoad, finalSystemLoad));
    }
  }
  
  private void updateSendStats(long count, long time) {
    totalSent.addAndGet(count);
    totalSendTime.addAndGet(time);
  }
  
  public void updateBinningStats(int count, long time, Map<String,TabletServerMutations> binnedMutations) {
    totalBinTime.addAndGet(time);
    totalBinned.addAndGet(count);
    updateBatchStats(binnedMutations);
  }
  
  private synchronized void updateBatchStats(Map<String,TabletServerMutations> binnedMutations) {
    tabletServersBatchSum += binnedMutations.size();
    
    minTabletServersBatch = Math.min(minTabletServersBatch, binnedMutations.size());
    maxTabletServersBatch = Math.max(maxTabletServersBatch, binnedMutations.size());
    
    int numTablets = 0;
    
    for (Entry<String,TabletServerMutations> entry : binnedMutations.entrySet()) {
      TabletServerMutations tsm = entry.getValue();
      numTablets += tsm.getMutations().size();
    }
    
    tabletBatchSum += numTablets;
    
    minTabletBatch = Math.min(minTabletBatch, numTablets);
    maxTabletBatch = Math.max(maxTabletBatch, numTablets);
    
    numBatches++;
  }
  
  private void waitRTE() {
    try {
      wait();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  
  // BEGIN code for handling unrecoverable errors
  
  private void updatedConstraintViolations(List<ConstraintViolationSummary> cvsList) {
    if (cvsList.size() > 0) {
      synchronized (this) {
        somethingFailed = true;
        violations.add(cvsList);
        this.notifyAll();
      }
    }
  }
  
  private void updateAuthorizationFailures(Collection<KeyExtent> authorizationFailures) {
    if (authorizationFailures.size() > 0) {
      
      // was a table deleted?
      HashSet<String> tableIds = new HashSet<String>();
      for (KeyExtent ke : authorizationFailures)
        tableIds.add(ke.getTableId().toString());
      
      Tables.clearCache(instance);
      for (String tableId : tableIds)
        if (!Tables.exists(instance, tableId))
          throw new TableDeletedException(tableId);
      
      synchronized (this) {
        somethingFailed = true;
        this.authorizationFailures.addAll(authorizationFailures);
        this.notifyAll();
      }
    }
  }
  
  private synchronized void updateServerErrors(String server, Exception e) {
    somethingFailed = true;
    this.serverSideErrors.add(server);
    this.notifyAll();
    log.error("Server side error on " + server);
  }
  
  private synchronized void updateUnknownErrors(String msg, Throwable t) {
    somethingFailed = true;
    unknownErrors++;
    this.lastUnknownError = t;
    this.notifyAll();
    if (t instanceof TableDeletedException || t instanceof TableOfflineException)
      log.debug(msg, t); // this is not unknown
    else
      log.error(msg, t);
  }
  
  private void checkForFailures() throws MutationsRejectedException {
    if (somethingFailed) {
      List<ConstraintViolationSummary> cvsList = violations.asList();
      throw new MutationsRejectedException(cvsList, new ArrayList<KeyExtent>(authorizationFailures), serverSideErrors, unknownErrors, lastUnknownError);
    }
  }
  
  // END code for handling unrecoverable errors
  
  // BEGIN code for handling failed mutations
  
  /**
   * Add mutations that previously failed back into the mix
   * 
   * @param mutationsprivate
   *          static final Logger log = Logger.getLogger(TabletServerBatchWriter.class);
   */
  private synchronized void addFailedMutations(MutationSet failedMutations) throws Exception {
    mutations.addAll(failedMutations);
    if (mutations.getMemoryUsed() >= maxMem / 2 || closed || flushing) {
      startProcessing();
    }
  }
  
  private class FailedMutations extends TimerTask {
    
    private MutationSet recentFailures = null;
    private long initTime;
    
    FailedMutations() {
      jtimer.schedule(this, 0, 500);
    }
    
    private MutationSet init() {
      if (recentFailures == null) {
        recentFailures = new MutationSet();
        initTime = System.currentTimeMillis();
      }
      return recentFailures;
    }
    
    synchronized void add(String table, ArrayList<Mutation> tableFailures) {
      init().addAll(table, tableFailures);
    }
    
    synchronized void add(MutationSet failures) {
      init().addAll(failures);
    }
    
    @Override
    public void run() {
      try {
        MutationSet rf = null;
        
        synchronized (this) {
          if (recentFailures != null && System.currentTimeMillis() - initTime > 1000) {
            rf = recentFailures;
            recentFailures = null;
          }
        }
        
        if (rf != null) {
          if (log.isTraceEnabled())
            log.trace("requeuing " + rf.size() + " failed mutations");
          addFailedMutations(rf);
        }
      } catch (Throwable t) {
        updateUnknownErrors("Failed to requeue failed mutations " + t.getMessage(), t);
        cancel();
      }
    }
  }
  
  // END code for handling failed mutations
  
  // BEGIN code for sending mutations to tablet servers using background threads
  
  private interface MutationBinner {
    Map<String,? extends DeliverableMutations> binMutations(MutationSet muts);
  }
  
  private interface DeliverableMutations {
    void add(DeliverableMutations muts);
    
    long getMemoryUsed();
    
    MutationSet send(String location) throws IOException, AccumuloSecurityException, AccumuloServerException;
    
    long getMutationCount();

    HashSet<String> getTableIDs();
    
    long getExtentCount();
    
    MutationSet toMutationSet();
  }
  
  /**
   * A set of mutations that are ready to be directly delivered to a tablet server.
   */
  class DirectlyDeliverableMutations implements DeliverableMutations {
    private static final int MUTATION_BATCH_SIZE = 1 << 17;
    
    TabletServerMutations mutations;
    
    public DirectlyDeliverableMutations(TabletServerMutations mutations) {
      this.mutations = mutations;
    }

    @Override
    public void add(DeliverableMutations muts) {
      DirectlyDeliverableMutations tsmuts = (DirectlyDeliverableMutations) muts;
      
      for (Entry<KeyExtent,List<Mutation>> entry2 : tsmuts.mutations.getMutations().entrySet()) {
        for (Mutation m : entry2.getValue()) {
          mutations.addMutation(entry2.getKey(), m);
        }
      }
    }
    
    @Override
    public long getMemoryUsed() {
      long bytes = 0;
      for (List<Mutation> ml : mutations.getMutations().values()) {
        for (Mutation mutation : ml) {
          bytes += mutation.estimatedMemoryUsed();
        }
      }
      
      return bytes;
    }
    
    @Override
    public HashSet<String> getTableIDs() {
      HashSet<String> tables = new HashSet<String>();
      for (KeyExtent ke : mutations.getMutations().keySet())
        tables.add(ke.getTableId().toString());
      return tables;
    }
    
    @Override
    public long getExtentCount() {
      return mutations.getMutations().size();
    }
    
    @Override
    public MutationSet toMutationSet() {
      MutationSet ms = new MutationSet();
      for (Entry<KeyExtent,List<Mutation>> entry2 : mutations.getMutations().entrySet()) {
        ms.addAll(entry2.getKey().getTableId().toString(), entry2.getValue());
      }
      return ms;
    }
    
    @Override
    public MutationSet send(String location) throws IOException, AccumuloSecurityException, AccumuloServerException {
      
      Map<KeyExtent,List<Mutation>> tabMuts = mutations.getMutations();
      
      if (tabMuts.size() == 0) {
        return new MutationSet();
      }
      
      try {
        TabletClientService.Iface client = ThriftUtil.getTServerClient(location, instance.getConfiguration());
        try {
          MutationSet allFailures = new MutationSet();
          
          if (tabMuts.size() == 1 && tabMuts.values().iterator().next().size() == 1) {
            Entry<KeyExtent,List<Mutation>> entry = tabMuts.entrySet().iterator().next();
            
            try {
              client.update(null, credentials, entry.getKey().toThrift(), entry.getValue().get(0).toThrift());
            } catch (NotServingTabletException e) {
              allFailures.addAll(entry.getKey().getTableId().toString(), entry.getValue());
              TabletLocator.getInstance(instance, credentials, new Text(entry.getKey().getTableId())).invalidateCache(entry.getKey());
            } catch (ConstraintViolationException e) {
              updatedConstraintViolations(Translator.translate(e.violationSummaries, Translator.TCVST));
            }
          } else {
            
            long usid = client.startUpdate(null, credentials);
            
            List<TMutation> updates = new ArrayList<TMutation>();
            for (Entry<KeyExtent,List<Mutation>> entry : tabMuts.entrySet()) {
              long size = 0;
              Iterator<Mutation> iter = entry.getValue().iterator();
              while (iter.hasNext()) {
                while (size < MUTATION_BATCH_SIZE && iter.hasNext()) {
                  Mutation mutation = iter.next();
                  updates.add(mutation.toThrift());
                  size += mutation.numBytes();
                }
                
                client.applyUpdates(null, usid, entry.getKey().toThrift(), updates);
                updates.clear();
                size = 0;
              }
            }
            
            UpdateErrors updateErrors = client.closeUpdate(null, usid);
            Map<KeyExtent,Long> failures = Translator.translate(updateErrors.failedExtents, Translator.TKET);
            updatedConstraintViolations(Translator.translate(updateErrors.violationSummaries, Translator.TCVST));
            updateAuthorizationFailures(Translator.translate(updateErrors.authorizationFailures, Translator.TKET));
            
            for (Entry<KeyExtent,Long> entry : failures.entrySet()) {
              KeyExtent failedExtent = entry.getKey();
              int numCommitted = (int) (long) entry.getValue();
              
              String table = failedExtent.getTableId().toString();
              
              TabletLocator.getInstance(instance, credentials, new Text(table)).invalidateCache(failedExtent);
              
              ArrayList<Mutation> mutations = (ArrayList<Mutation>) tabMuts.get(failedExtent);
              allFailures.addAll(table, mutations.subList(numCommitted, mutations.size()));
            }
          }
          return allFailures;
        } finally {
          ThriftUtil.returnClient((TServiceClient) client);
        }
      } catch (TTransportException e) {
        throw new IOException(e);
      } catch (TApplicationException tae) {
        updateServerErrors(location, tae);
        throw new AccumuloServerException(location, tae);
      } catch (ThriftSecurityException e) {
        updateAuthorizationFailures(tabMuts.keySet());
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (TException e) {
        throw new IOException(e);
      } catch (NoSuchScanIDException e) {
        throw new IOException(e);
      }
    }
    
    public long getMutationCount() {
      long count = 0;
      for (List<Mutation> list : mutations.getMutations().values()) {
        count += list.size();
      }
      
      return count;
    }
  }
  
  class IndirectlyDeliverableMutations implements DeliverableMutations {
    private static final int MUTATION_BATCH_SIZE = 1 << 17;
    private Map<String,TabletServerMutations> mutations;
    
    public IndirectlyDeliverableMutations(Map<String,TabletServerMutations> mutations) {
      this.mutations = mutations;
    }
    
    @Override
    public void add(DeliverableMutations muts) {
      IndirectlyDeliverableMutations idm = (IndirectlyDeliverableMutations) muts;
      
      for (Entry<String,TabletServerMutations> entry : idm.mutations.entrySet()) {
        TabletServerMutations existing = mutations.get(entry.getKey());
        if (existing == null) {
          mutations.put(entry.getKey(), entry.getValue());
        } else {
          for (Entry<KeyExtent,List<Mutation>> entry2 : entry.getValue().getMutations().entrySet()) {
            for (Mutation m : entry2.getValue()) {
              existing.addMutation(entry2.getKey(), m);
            }
          }
        }
      }
    }
    
    @Override
    public long getMemoryUsed() {
      long bytes = 0;
      for (TabletServerMutations tsm : mutations.values()) {
        for (List<Mutation> ml : tsm.getMutations().values()) {
          for (Mutation mutation : ml) {
            bytes += mutation.estimatedMemoryUsed();
          }
        }
      }
      return bytes;
    }
    
    @Override
    public MutationSet send(String location) throws IOException, AccumuloSecurityException, AccumuloServerException {
      if (mutations.size() == 0) {
        return new MutationSet();
      }
      
      try {
        TabletClientService.Iface client = ThriftUtil.getTServerClient(location, instance.getConfiguration());
        try {
          MutationSet allFailures = new MutationSet();
          
          long rupid = client.startRackUpdate(null, credentials);
          
          for (Entry<String,TabletServerMutations> entry : mutations.entrySet()) {
            
            client.setRackUpdateServer(null, rupid, entry.getKey());

            List<TMutation> updates = new ArrayList<TMutation>();
            for (Entry<KeyExtent,List<Mutation>> entry2 : entry.getValue().getMutations().entrySet()) {
              long size = 0;
              Iterator<Mutation> iter = entry2.getValue().iterator();
              while (iter.hasNext()) {
                while (size < MUTATION_BATCH_SIZE && iter.hasNext()) {
                  Mutation mutation = iter.next();
                  updates.add(mutation.toThrift());
                  size += mutation.numBytes();
                }
                
                client.applyRackUpdates(null, rupid, entry2.getKey().toThrift(), updates);
                updates.clear();
                size = 0;
              }
            }
          }
          
          client.closeRackUpdate(null, rupid);
          
          return allFailures;
        } finally {
          ThriftUtil.returnClient((TServiceClient) client);
        }
      } catch (TTransportException e) {
        throw new IOException(e);
      } catch (TApplicationException tae) {
        updateServerErrors(location, tae);
        throw new AccumuloServerException(location, tae);
      } catch (ThriftSecurityException e) {
        // TODO
        // updateAuthorizationFailures(tabMuts.keySet());
        throw new AccumuloSecurityException(e.user, e.code, e);
      } catch (TException e) {
        throw new IOException(e);
      } catch (NoSuchScanIDException e) {
        throw new IOException(e);
      }
    }
    
    @Override
    public long getMutationCount() {
      long count = 0;
      for (TabletServerMutations tsm : mutations.values()) {
        for (List<Mutation> list : tsm.getMutations().values()) {
          count += list.size();
        }
      }
      return count;
    }
    
    @Override
    public HashSet<String> getTableIDs() {
      HashSet<String> tables = new HashSet<String>();
      for (TabletServerMutations tsm : mutations.values())
        for (KeyExtent ke : tsm.getMutations().keySet())
          tables.add(ke.getTableId().toString());
      return tables;
    }
    
    @Override
    public long getExtentCount() {
      long count = 0;
      for (TabletServerMutations tsm : mutations.values())
        count += tsm.getMutations().size();
      return count;
    }
    
    @Override
    public MutationSet toMutationSet() {
      MutationSet ms = new MutationSet();
      for (TabletServerMutations tsm : mutations.values()) {
        for (Entry<KeyExtent,List<Mutation>> entry2 : tsm.getMutations().entrySet()) {
          ms.addAll(entry2.getKey().getTableId().toString(), entry2.getValue());
        }
      }
      return ms;
    }
    
  }

  private Map<String,TabletServerMutations> binMutations(MutationSet mutationsToProcess) {
    HashMap<String,TabletServerMutations> binnedMutations = new HashMap<String,TabletServerMutations>();
    
    try {
      Set<Entry<String,List<Mutation>>> es = mutationsToProcess.getMutations().entrySet();
      for (Entry<String,List<Mutation>> entry : es) {
        TabletLocator locator = TabletLocator.getInstance(instance, credentials, new Text(entry.getKey()));
        
        String table = entry.getKey();
        List<Mutation> tableMutations = entry.getValue();
        
        if (tableMutations != null) {
          ArrayList<Mutation> tableFailures = new ArrayList<Mutation>();
          locator.binMutations(tableMutations, binnedMutations, tableFailures);
          
          if (tableFailures.size() > 0) {
            failedMutations.add(table, tableFailures);
            
            if (tableFailures.size() == tableMutations.size())
              if (!Tables.exists(instance, entry.getKey()))
                throw new TableDeletedException(entry.getKey());
              else if (Tables.getTableState(instance, table) == TableState.OFFLINE)
                throw new TableOfflineException(instance, entry.getKey());
          }
        }
        
      }
      return binnedMutations;
    } catch (AccumuloServerException ase) {
      updateServerErrors(ase.getServer(), ase);
    } catch (AccumuloException ae) {
      // assume an IOError communicating with !METADATA tablet
      failedMutations.add(mutationsToProcess);
    } catch (AccumuloSecurityException e) {
      updateAuthorizationFailures(Collections.singletonList(new KeyExtent(new Text(Constants.METADATA_TABLE_ID), null, null)));
    } catch (TableDeletedException e) {
      updateUnknownErrors(e.getMessage(), e);
    } catch (TableOfflineException e) {
      updateUnknownErrors(e.getMessage(), e);
    } catch (TableNotFoundException e) {
      updateUnknownErrors(e.getMessage(), e);
    }
    
    // an error ocurred
    binnedMutations.clear();
    return binnedMutations;
  }

  /**
   * A binner that bins mutations for direct delviery to a tablet server
   */

  private class TabletServerMutationBinner implements MutationBinner {
    
    @Override
    public Map<String,? extends DeliverableMutations> binMutations(MutationSet mutationsToProcess) {
      
      Map<String,TabletServerMutations> tmp = TabletServerBatchWriter.this.binMutations(mutationsToProcess);

      Map<String,DirectlyDeliverableMutations> binnedMutations = new HashMap<String,DirectlyDeliverableMutations>();
      
      Set<Entry<String,TabletServerMutations>> es = tmp.entrySet();
      
      for (Entry<String,TabletServerMutations> entry : es) {
        binnedMutations.put(entry.getKey(), new DirectlyDeliverableMutations(entry.getValue()));
      }
      
      return binnedMutations;
    }
    
  }

  /**
   * A mutation binner that bins mutations for indirect delivery to a random node on a rack that will do the final delivery.
   */
  private class RackMutationBinner implements MutationBinner {
    
    private DNSToSwitchMapping rackMapping;

    RackMutationBinner(DNSToSwitchMapping mapping) {
      this.rackMapping = mapping;
    }

    @Override
    public Map<String,? extends DeliverableMutations> binMutations(MutationSet muts) {
      
      Map<String,IndirectlyDeliverableMutations> binnedMutations = new HashMap<String,TabletServerBatchWriter.IndirectlyDeliverableMutations>();
      
      try {
        Map<String,TabletServerMutations> tmp = TabletServerBatchWriter.this.binMutations(muts);
        Map<String,Map<String,TabletServerMutations>> tmp2 = RackUtil.binToRack(rackMapping,
            new ServerSelector(instance.getConnector(credentials), rackMapping), tmp);
        
        Set<Entry<String,Map<String,TabletServerMutations>>> es = tmp2.entrySet();
        for (Entry<String,Map<String,TabletServerMutations>> entry : es) {
          binnedMutations.put(entry.getKey(), new IndirectlyDeliverableMutations(entry.getValue()));
        }
        
        return binnedMutations;
      } catch (AccumuloException e) {
        // assume an IOError communicating with !METADATA tablet
        failedMutations.add(muts);
      } catch (AccumuloSecurityException e) {
        updateAuthorizationFailures(Collections.singletonList(new KeyExtent(new Text(Constants.METADATA_TABLE_ID), null, null)));
      }
      
      binnedMutations.clear();
      return binnedMutations;
    }
    
  }

  private class MutationWriter {

    private ExecutorService sendThreadPool;
    private Map<String,DeliverableMutations> serversMutations;
    private Set<String> queued;
    private MutationBinner mutationBinner;
    
    public MutationWriter(MutationBinner mutationBinner, int numSendThreads) {
      serversMutations = new HashMap<String,DeliverableMutations>();

      queued = new HashSet<String>();
      sendThreadPool = Executors.newFixedThreadPool(numSendThreads, new NamingThreadFactory(this.getClass().getName()));
      this.mutationBinner = mutationBinner;
    }
    
    void addMutations(MutationSet mutationsToSend) {
      Map<String,? extends DeliverableMutations> binnedMutations;
      Span span = Trace.start("binMutations");
      try {
        long t1 = System.currentTimeMillis();
        binnedMutations = mutationBinner.binMutations(mutationsToSend);
        long t2 = System.currentTimeMillis();
        // TODO update stats
        // updateBinningStats(mutationsToSend.size(), (t2 - t1), binnedMutations);
      } finally {
        span.stop();
      }
      addMutations(binnedMutations);
    }
    
    private synchronized void addMutations(Map<String,? extends DeliverableMutations> binnedMutations) {
      
      int count = 0;
      
      // merge mutations into existing mutations for a tablet server
      for (Entry<String,? extends DeliverableMutations> entry : binnedMutations.entrySet()) {
        String server = entry.getKey();
        
        DeliverableMutations currentMutations = serversMutations.get(server);
        
        if (currentMutations == null) {
          serversMutations.put(server, entry.getValue());
        } else {
          currentMutations.add(entry.getValue());
        }
        
        if (log.isTraceEnabled())
          count += entry.getValue().getMutationCount();
      }
      
      if (count > 0 && log.isTraceEnabled())
        log.trace(String.format("Started sending %,d mutations to %,d tablet servers", count, binnedMutations.keySet().size()));
      
      // randomize order of servers
      ArrayList<String> servers = new ArrayList<String>(binnedMutations.keySet());
      Collections.shuffle(servers);
      
      for (String server : servers)
        if (!queued.contains(server)) {
          sendThreadPool.submit(Trace.wrap(new SendTask(server)));
          queued.add(server);
        }
    }
    
    private synchronized DeliverableMutations getMutationsToSend(String server) {
      DeliverableMutations tsmuts = serversMutations.remove(server);
      if (tsmuts == null)
        queued.remove(server);
      
      return tsmuts;
    }
    
    class SendTask implements Runnable {
      
      private String location;
      
      SendTask(String server) {
        this.location = server;
      }
      
      @Override
      public void run() {
        try {
          DeliverableMutations tsmuts = getMutationsToSend(location);
          
          while (tsmuts != null) {
            send(tsmuts);
            tsmuts = getMutationsToSend(location);
          }
          
          return;
        } catch (Throwable t) {
          updateUnknownErrors("Failed to send tablet server " + location + " its batch : " + t.getMessage(), t);
        }
      }
      
      public void send(DeliverableMutations tsm) throws AccumuloServerException, AccumuloSecurityException {
        
        MutationSet failures = null;
        
        String oldName = Thread.currentThread().getName();
        

        try {
          
          long count = 0;
          count += tsm.getMutationCount();

          String msg = "sending " + String.format("%,d", count) + " mutations to " + String.format("%,d", tsm.getExtentCount()) + " tablets at " + location;
          Thread.currentThread().setName(msg);
          
          Span span = Trace.start("sendMutations");
          try {
            long st1 = System.currentTimeMillis();
            failures = tsm.send(location);
            long st2 = System.currentTimeMillis();
            if (log.isTraceEnabled())
              log.trace("sent " + String.format("%,d", count) + " mutations to " + location + " in "
                  + String.format("%.2f secs (%,.2f mutations/sec) with %,d failures", (st2 - st1) / 1000.0, count / ((st2 - st1) / 1000.0), failures.size()));
            
            long successBytes = tsm.getMemoryUsed();
            
            if (failures.size() > 0) {
              failedMutations.add(failures);
              successBytes -= failures.getMemoryUsed();
            }
            
            updateSendStats(count, st2 - st1);
            decrementMemUsed(successBytes);
            
          } finally {
            span.stop();
          }
        } catch (IOException e) {
          if (log.isTraceEnabled())
            log.trace("failed to send mutations to " + location + " : " + e.getMessage());
          
          HashSet<String> tables = tsm.getTableIDs();
          
          for (String table : tables)
            TabletLocator.getInstance(instance, credentials, new Text(table)).invalidateCache(location);
          
          failedMutations.add(tsm.toMutationSet());
        } finally {
          Thread.currentThread().setName(oldName);
        }
      }
    }
    
    
  }
  
  // END code for sending mutations to tablet servers using background threads
  
  private static class MutationSet {
    
    private HashMap<String,List<Mutation>> mutations;
    private int memoryUsed = 0;
    
    MutationSet() {
      mutations = new HashMap<String,List<Mutation>>();
    }
    
    void addMutation(String table, Mutation mutation) {
      List<Mutation> tabMutList = mutations.get(table);
      if (tabMutList == null) {
        tabMutList = new ArrayList<Mutation>();
        mutations.put(table, tabMutList);
      }
      
      tabMutList.add(mutation);
      
      memoryUsed += mutation.estimatedMemoryUsed();
    }
    
    Map<String,List<Mutation>> getMutations() {
      return mutations;
    }
    
    int size() {
      int result = 0;
      for (List<Mutation> perTable : mutations.values()) {
        result += perTable.size();
      }
      return result;
    }
    
    public void addAll(MutationSet failures) {
      Set<Entry<String,List<Mutation>>> es = failures.getMutations().entrySet();
      
      for (Entry<String,List<Mutation>> entry : es) {
        String table = entry.getKey();
        
        for (Mutation mutation : entry.getValue()) {
          addMutation(table, mutation);
        }
      }
    }
    
    public void addAll(String table, List<Mutation> mutations) {
      for (Mutation mutation : mutations) {
        addMutation(table, mutation);
      }
    }
    
    public int getMemoryUsed() {
      return memoryUsed;
    }
    
  }
}
