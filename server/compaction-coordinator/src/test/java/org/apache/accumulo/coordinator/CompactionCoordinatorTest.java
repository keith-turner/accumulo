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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.CompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.compaction.ExternalCompactionUtil;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.hadoop.shaded.org.apache.commons.compress.utils.Sets;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CompactionCoordinator.class, DeadCompactionDetector.class, ThriftUtil.class,
    ExternalCompactionUtil.class})
@SuppressStaticInitializationFor({"org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.slf4j.*", "org.apache.logging.*", "org.apache.log4j.*",
    "org.apache.commons.logging.*", "org.xml.*", "javax.xml.*", "org.w3c.dom.*",
    "com.sun.org.apache.xerces.*"})
public class CompactionCoordinatorTest {

  public class TestCoordinator extends CompactionCoordinator {

    private final ServerContext ctx;
    private final AccumuloConfiguration conf;
    private final ServerAddress client;
    private final TabletClientService.Client tabletServerClient;

    protected TestCoordinator(AccumuloConfiguration conf, CompactionFinalizer finalizer,
        LiveTServerSet tservers, ServerAddress client,
        TabletClientService.Client tabletServerClient, ServerContext ctx,
        AuditedSecurityOperation security) {
      super(new ServerOpts(), new String[] {});
      this.conf = conf;
      this.compactionFinalizer = finalizer;
      this.tserverSet = tservers;
      this.client = client;
      this.tabletServerClient = tabletServerClient;
      this.ctx = ctx;
      this.security = security;
    }

    @Override
    protected long getTServerCheckInterval() {
      this.shutdown = true;
      return 0L;
    }

    @Override
    protected CompactionFinalizer createCompactionFinalizer() {
      return null;
    }

    @Override
    protected LiveTServerSet createLiveTServerSet() {
      return null;
    }

    @Override
    protected void setupSecurity() {}

    @Override
    protected void startGCLogger() {}

    @Override
    protected void printStartupMsg() {}

    @Override
    public AccumuloConfiguration getConfiguration() {
      return this.conf;
    }

    @Override
    public ServerContext getContext() {
      return this.ctx;
    }

    @Override
    protected void getCoordinatorLock(HostAndPort clientAddress)
        throws KeeperException, InterruptedException {}

    @Override
    protected ServerAddress startCoordinatorClientService() throws UnknownHostException {
      return client;
    }

    @Override
    protected Client getTabletServerConnection(TServerInstance tserver) throws TTransportException {
      return tabletServerClient;
    }

    @Override
    protected org.apache.accumulo.core.compaction.thrift.Compactor.Client
        getCompactorConnection(HostAndPort compactorAddress) throws TTransportException {
      // TODO Auto-generated method stub
      return super.getCompactorConnection(compactorAddress);
    }

    @Override
    public void compactionCompleted(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TKeyExtent textent, CompactionStats stats) throws TException {}

    @Override
    public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
        TKeyExtent extent) throws TException {}

    public Map<String,TreeMap<Long,LinkedHashSet<TServerInstance>>> getQueues() {
      return QUEUES;
    }

    public Map<TServerInstance,HashSet<QueueAndPriority>> getIndex() {
      return INDEX;
    }

    public Map<ExternalCompactionId,RunningCompaction> getRunning() {
      return RUNNING;
    }

    @Override
    protected TabletMetadata getMetadataEntryForExtent(KeyExtent extent) {
      return new TabletMetadata() {
        @Override
        public Location getLocation() {
          return new Location("localhost:9997", "", LocationType.CURRENT);
        }
      };
    }

  }

  @Test
  public void testCoordinatorColdStartNoCompactions() throws Exception {
    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));
    PowerMock.suppress(PowerMock.methods(ThriftUtil.class, "returnClient"));
    PowerMock.suppress(PowerMock.methods(DeadCompactionDetector.class, "detectDeadCompactions"));

    AccumuloConfiguration conf = PowerMock.createNiceMock(AccumuloConfiguration.class);
    ServerContext ctx = PowerMock.createNiceMock(ServerContext.class);

    CompactionFinalizer finalizer = PowerMock.createNiceMock(CompactionFinalizer.class);
    LiveTServerSet tservers = PowerMock.createNiceMock(LiveTServerSet.class);
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Collections.emptySet()).anyTimes();

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    EasyMock.expect(client.getAddress()).andReturn(address).anyTimes();

    TServerInstance tsi = PowerMock.createNiceMock(TServerInstance.class);
    EasyMock.expect(tsi.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletClientService.Client tsc = PowerMock.createNiceMock(TabletClientService.Client.class);
    EasyMock.expect(tsc.getCompactionQueueInfo(EasyMock.anyObject(), EasyMock.anyObject()))
        .andReturn(Collections.emptyList()).anyTimes();

    AuditedSecurityOperation security = PowerMock.createNiceMock(AuditedSecurityOperation.class);

    PowerMock.replayAll();

    TestCoordinator coordinator =
        new TestCoordinator(conf, finalizer, tservers, client, tsc, ctx, security);
    Assert.assertEquals(0, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    Assert.assertEquals(0, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(0, coordinator.getRunning().size());

    PowerMock.verifyAll();
    coordinator.getQueues().clear();
    coordinator.getIndex().clear();
    coordinator.getRunning().clear();
    coordinator.close();
  }

  @Test
  public void testCoordinatorColdStart() throws Exception {
    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));
    PowerMock.suppress(PowerMock.methods(ThriftUtil.class, "returnClient"));
    PowerMock.suppress(PowerMock.methods(DeadCompactionDetector.class, "detectDeadCompactions"));

    AccumuloConfiguration conf = PowerMock.createNiceMock(AccumuloConfiguration.class);
    ServerContext ctx = PowerMock.createNiceMock(ServerContext.class);
    TCredentials creds = PowerMock.createNiceMock(TCredentials.class);
    EasyMock.expect(ctx.rpcCreds()).andReturn(creds);

    CompactionFinalizer finalizer = PowerMock.createNiceMock(CompactionFinalizer.class);
    LiveTServerSet tservers = PowerMock.createNiceMock(LiveTServerSet.class);
    TServerInstance instance = PowerMock.createNiceMock(TServerInstance.class);
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Collections.emptySet()).once();
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Collections.singleton(instance)).once();

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    EasyMock.expect(client.getAddress()).andReturn(address).anyTimes();

    TServerInstance tsi = PowerMock.createNiceMock(TServerInstance.class);
    EasyMock.expect(tsi.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletClientService.Client tsc = PowerMock.createNiceMock(TabletClientService.Client.class);
    TCompactionQueueSummary queueSummary = PowerMock.createNiceMock(TCompactionQueueSummary.class);
    EasyMock.expect(tsc.getCompactionQueueInfo(EasyMock.anyObject(), EasyMock.anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    EasyMock.expect(queueSummary.getQueue()).andReturn("R2DQ");
    EasyMock.expect(queueSummary.getPriority()).andReturn(1L);

    AuditedSecurityOperation security = PowerMock.createNiceMock(AuditedSecurityOperation.class);

    PowerMock.replayAll();

    TestCoordinator coordinator =
        new TestCoordinator(conf, finalizer, tservers, client, tsc, ctx, security);
    Assert.assertEquals(0, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    Assert.assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), 1L);
    Map<Long,LinkedHashSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    Assert.assertNotNull(m);
    Assert.assertEquals(1, m.size());
    Assert.assertTrue(m.containsKey(1L));
    Set<TServerInstance> t = m.get(1L);
    Assert.assertNotNull(t);
    Assert.assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    Assert.assertEquals(tsi.getHostPortSession(), queuedTsi.getHostPortSession());
    Assert.assertEquals(1, coordinator.getIndex().size());
    Assert.assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(qp, i.iterator().next());
    Assert.assertEquals(0, coordinator.getRunning().size());

    PowerMock.verifyAll();
    coordinator.getQueues().clear();
    coordinator.getIndex().clear();
    coordinator.getRunning().clear();
    coordinator.close();
  }

  @Test
  public void testCoordinatorRestartNoRunningCompactions() throws Exception {
    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));
    PowerMock.suppress(PowerMock.methods(ThriftUtil.class, "returnClient"));
    PowerMock.suppress(PowerMock.methods(DeadCompactionDetector.class, "detectDeadCompactions"));

    AccumuloConfiguration conf = PowerMock.createNiceMock(AccumuloConfiguration.class);
    ServerContext ctx = PowerMock.createNiceMock(ServerContext.class);
    TCredentials creds = PowerMock.createNiceMock(TCredentials.class);
    EasyMock.expect(ctx.rpcCreds()).andReturn(creds);

    CompactionFinalizer finalizer = PowerMock.createNiceMock(CompactionFinalizer.class);
    LiveTServerSet tservers = PowerMock.createNiceMock(LiveTServerSet.class);
    tservers.scanServers();
    TServerInstance instance = PowerMock.createNiceMock(TServerInstance.class);
    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");
    EasyMock.expect(instance.getHostAndPort()).andReturn(tserverAddress).anyTimes();
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Sets.newHashSet(instance)).once();
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Sets.newHashSet(instance)).once();
    tservers.startListeningForTabletServerChanges();

    PowerMock.mockStatic(ExternalCompactionUtil.class);
    Map<HostAndPort,TExternalCompactionJob> runningCompactions = new HashMap<>();
    // ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    // TExternalCompactionJob job = PowerMock.createNiceMock(TExternalCompactionJob.class);
    // runningCompactions.put(tserverAddress, job);
    EasyMock.expect(ExternalCompactionUtil.getCompactionsRunningOnCompactors(ctx))
        .andReturn(runningCompactions);

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    EasyMock.expect(client.getAddress()).andReturn(address).anyTimes();

    EasyMock.expect(instance.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletClientService.Client tsc = PowerMock.createNiceMock(TabletClientService.Client.class);
    TCompactionQueueSummary queueSummary = PowerMock.createNiceMock(TCompactionQueueSummary.class);
    EasyMock.expect(tsc.getCompactionQueueInfo(EasyMock.anyObject(), EasyMock.anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    EasyMock.expect(queueSummary.getQueue()).andReturn("R2DQ");
    EasyMock.expect(queueSummary.getPriority()).andReturn(1L);

    AuditedSecurityOperation security = PowerMock.createNiceMock(AuditedSecurityOperation.class);

    PowerMock.replayAll();

    TestCoordinator coordinator =
        new TestCoordinator(conf, finalizer, tservers, client, tsc, ctx, security);
    Assert.assertEquals(0, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    Assert.assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), 1L);
    Map<Long,LinkedHashSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    Assert.assertNotNull(m);
    Assert.assertEquals(1, m.size());
    Assert.assertTrue(m.containsKey(1L));
    Set<TServerInstance> t = m.get(1L);
    Assert.assertNotNull(t);
    Assert.assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    Assert.assertEquals(instance.getHostPortSession(), queuedTsi.getHostPortSession());
    Assert.assertEquals(1, coordinator.getIndex().size());
    Assert.assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(qp, i.iterator().next());
    Assert.assertEquals(0, coordinator.getRunning().size());

    PowerMock.verifyAll();
    coordinator.getQueues().clear();
    coordinator.getIndex().clear();
    coordinator.getRunning().clear();
    coordinator.close();
  }

  @Test
  @Ignore
  public void testCoordinatorRestartOneRunningCompaction() throws Exception {
    // TODO: Need to finish this test.

    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));
    PowerMock.suppress(PowerMock.methods(ThriftUtil.class, "returnClient"));
    PowerMock.suppress(PowerMock.methods(DeadCompactionDetector.class, "detectDeadCompactions"));

    AccumuloConfiguration conf = PowerMock.createNiceMock(AccumuloConfiguration.class);
    ServerContext ctx = PowerMock.createNiceMock(ServerContext.class);
    TCredentials creds = PowerMock.createNiceMock(TCredentials.class);
    EasyMock.expect(ctx.rpcCreds()).andReturn(creds);

    CompactionFinalizer finalizer = PowerMock.createNiceMock(CompactionFinalizer.class);
    LiveTServerSet tservers = PowerMock.createNiceMock(LiveTServerSet.class);
    tservers.scanServers();
    TServerInstance instance = PowerMock.createNiceMock(TServerInstance.class);
    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");
    EasyMock.expect(instance.getHostAndPort()).andReturn(tserverAddress).anyTimes();
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Sets.newHashSet(instance)).once();
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Sets.newHashSet(instance)).once();
    tservers.startListeningForTabletServerChanges();

    PowerMock.mockStatic(ExternalCompactionUtil.class);
    Map<HostAndPort,TExternalCompactionJob> runningCompactions = new HashMap<>();
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob job = PowerMock.createNiceMock(TExternalCompactionJob.class);
    TKeyExtent extent = new TKeyExtent();
    extent.setTable("1".getBytes());
    EasyMock.expect(job.getExtent()).andReturn(extent);
    runningCompactions.put(tserverAddress, job);
    EasyMock.expect(ExternalCompactionUtil.getCompactionsRunningOnCompactors(ctx))
        .andReturn(runningCompactions);

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    EasyMock.expect(client.getAddress()).andReturn(address).anyTimes();

    EasyMock.expect(instance.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletClientService.Client tsc = PowerMock.createNiceMock(TabletClientService.Client.class);
    TCompactionQueueSummary queueSummary = PowerMock.createNiceMock(TCompactionQueueSummary.class);
    EasyMock.expect(tsc.getCompactionQueueInfo(EasyMock.anyObject(), EasyMock.anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    EasyMock.expect(queueSummary.getQueue()).andReturn("R2DQ");
    EasyMock.expect(queueSummary.getPriority()).andReturn(1L);

    AuditedSecurityOperation security = PowerMock.createNiceMock(AuditedSecurityOperation.class);

    PowerMock.replayAll();

    TestCoordinator coordinator =
        new TestCoordinator(conf, finalizer, tservers, client, tsc, ctx, security);
    Assert.assertEquals(0, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    Assert.assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), 1L);
    Map<Long,LinkedHashSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    Assert.assertNotNull(m);
    Assert.assertEquals(1, m.size());
    Assert.assertTrue(m.containsKey(1L));
    Set<TServerInstance> t = m.get(1L);
    Assert.assertNotNull(t);
    Assert.assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    Assert.assertEquals(instance.getHostPortSession(), queuedTsi.getHostPortSession());
    Assert.assertEquals(1, coordinator.getIndex().size());
    Assert.assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(qp, i.iterator().next());
    Assert.assertEquals(0, coordinator.getRunning().size());

    PowerMock.verifyAll();
    coordinator.getQueues().clear();
    coordinator.getIndex().clear();
    coordinator.getRunning().clear();
    coordinator.close();
  }

  @Test
  public void testGetCompactionJob() throws Exception {
    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));
    PowerMock.suppress(PowerMock.methods(ThriftUtil.class, "returnClient"));
    PowerMock.suppress(PowerMock.methods(DeadCompactionDetector.class, "detectDeadCompactions"));

    AccumuloConfiguration conf = PowerMock.createNiceMock(AccumuloConfiguration.class);
    ServerContext ctx = PowerMock.createNiceMock(ServerContext.class);
    TCredentials creds = PowerMock.createNiceMock(TCredentials.class);
    EasyMock.expect(ctx.rpcCreds()).andReturn(creds).anyTimes();

    CompactionFinalizer finalizer = PowerMock.createNiceMock(CompactionFinalizer.class);
    LiveTServerSet tservers = PowerMock.createNiceMock(LiveTServerSet.class);
    TServerInstance instance = PowerMock.createNiceMock(TServerInstance.class);
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Collections.emptySet()).once();
    EasyMock.expect(tservers.getCurrentServers()).andReturn(Collections.singleton(instance)).once();
    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");
    EasyMock.expect(instance.getHostAndPort()).andReturn(tserverAddress).anyTimes();

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    EasyMock.expect(client.getAddress()).andReturn(address).anyTimes();

    TServerInstance tsi = PowerMock.createNiceMock(TServerInstance.class);
    EasyMock.expect(tsi.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletClientService.Client tsc = PowerMock.createNiceMock(TabletClientService.Client.class);
    TCompactionQueueSummary queueSummary = PowerMock.createNiceMock(TCompactionQueueSummary.class);
    EasyMock.expect(tsc.getCompactionQueueInfo(EasyMock.anyObject(), EasyMock.anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    EasyMock.expect(queueSummary.getQueue()).andReturn("R2DQ");
    EasyMock.expect(queueSummary.getPriority()).andReturn(1L);

    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob job = PowerMock.createNiceMock(TExternalCompactionJob.class);
    EasyMock.expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    TInfo trace = Whitebox.getInternalState(TraceUtil.class, "DONT_TRACE");
    EasyMock
        .expect(
            tsc.reserveCompactionJob(trace, creds, "R2DQ", 1, "localhost:10241", eci.toString()))
        .andReturn(job).anyTimes();

    AuditedSecurityOperation security = PowerMock.createNiceMock(AuditedSecurityOperation.class);
    EasyMock.expect(security.canPerformSystemActions(creds)).andReturn(true);

    PowerMock.replayAll();

    TestCoordinator coordinator =
        new TestCoordinator(conf, finalizer, tservers, client, tsc, ctx, security);
    Assert.assertEquals(0, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(0, coordinator.getRunning().size());
    // Use coordinator.run() to populate the internal data structures. This is tested in a different
    // test.
    coordinator.run();

    Assert.assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), 1L);
    Map<Long,LinkedHashSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    Assert.assertNotNull(m);
    Assert.assertEquals(1, m.size());
    Assert.assertTrue(m.containsKey(1L));
    Set<TServerInstance> t = m.get(1L);
    Assert.assertNotNull(t);
    Assert.assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    Assert.assertEquals(tsi.getHostPortSession(), queuedTsi.getHostPortSession());
    Assert.assertEquals(1, coordinator.getIndex().size());
    Assert.assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(qp, i.iterator().next());
    Assert.assertEquals(0, coordinator.getRunning().size());

    // Get the next job
    TExternalCompactionJob createdJob =
        coordinator.getCompactionJob(trace, creds, "R2DQ", "localhost:10241", eci.toString());
    Assert.assertEquals(eci.toString(), createdJob.getExternalCompactionId());

    Assert.assertEquals(1, coordinator.getQueues().size());
    Assert.assertEquals(0, coordinator.getIndex().size());
    Assert.assertEquals(1, coordinator.getRunning().size());
    Entry<ExternalCompactionId,RunningCompaction> entry =
        coordinator.getRunning().entrySet().iterator().next();
    Assert.assertEquals(eci.toString(), entry.getKey().toString());
    Assert.assertEquals("localhost:10241", entry.getValue().getCompactorAddress());
    Assert.assertEquals(eci.toString(), entry.getValue().getJob().getExternalCompactionId());

    PowerMock.verifyAll();
    coordinator.getQueues().clear();
    coordinator.getIndex().clear();
    coordinator.getRunning().clear();
    coordinator.close();

  }

  @Test
  public void testGetCompactionJobNoJobs() throws Exception {
    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));

    AccumuloConfiguration conf = PowerMock.createNiceMock(AccumuloConfiguration.class);
    ServerContext ctx = PowerMock.createNiceMock(ServerContext.class);
    TCredentials creds = PowerMock.createNiceMock(TCredentials.class);

    CompactionFinalizer finalizer = PowerMock.createNiceMock(CompactionFinalizer.class);
    LiveTServerSet tservers = PowerMock.createNiceMock(LiveTServerSet.class);

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    EasyMock.expect(client.getAddress()).andReturn(address).anyTimes();

    TabletClientService.Client tsc = PowerMock.createNiceMock(TabletClientService.Client.class);

    AuditedSecurityOperation security = PowerMock.createNiceMock(AuditedSecurityOperation.class);
    EasyMock.expect(security.canPerformSystemActions(creds)).andReturn(true);

    PowerMock.replayAll();

    TestCoordinator coordinator =
        new TestCoordinator(conf, finalizer, tservers, client, tsc, ctx, security);
    coordinator.getQueues().clear();
    coordinator.getIndex().clear();
    coordinator.getRunning().clear();
    TExternalCompactionJob job = coordinator.getCompactionJob(TraceUtil.traceInfo(), creds, "R2DQ",
        "localhost:10240", UUID.randomUUID().toString());
    Assert.assertNull(job.getExternalCompactionId());

    PowerMock.verifyAll();
    coordinator.close();
  }

}
