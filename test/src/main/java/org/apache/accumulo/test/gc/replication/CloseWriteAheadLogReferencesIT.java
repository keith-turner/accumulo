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
package org.apache.accumulo.test.gc.replication;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.gc.replication.CloseWriteAheadLogReferences;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class CloseWriteAheadLogReferencesIT extends ConfigurableMacBase {

  private WrappedCloseWriteAheadLogReferences refs;
  private Connector conn;

  private static class WrappedCloseWriteAheadLogReferences extends CloseWriteAheadLogReferences {
    public WrappedCloseWriteAheadLogReferences(ServerContext context) {
      super(context);
    }

    @Override
    protected long updateReplicationEntries(Connector conn, Set<String> closedWals) {
      return super.updateReplicationEntries(conn, closedWals);
    }
  }

  @Before
  public void setupInstance() throws Exception {
    conn = getConnector();
    conn.securityOperations().grantTablePermission(conn.whoami(), ReplicationTable.NAME,
        TablePermission.WRITE);
    conn.securityOperations().grantTablePermission(conn.whoami(), MetadataTable.NAME,
        TablePermission.WRITE);
    ReplicationTable.setOnline(conn);
  }

  @Before
  public void setupEasyMockStuff() {
    SiteConfiguration siteConfig = EasyMock.createMock(SiteConfiguration.class);
    final AccumuloConfiguration systemConf = new ConfigurationCopy(new HashMap<>());
    ServerConfigurationFactory factory = createMock(ServerConfigurationFactory.class);
    expect(factory.getSystemConfiguration()).andReturn(systemConf).anyTimes();
    expect(factory.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    // Just make the SiteConfiguration delegate to our AccumuloConfiguration
    // Presently, we only need get(Property) and iterator().
    EasyMock.expect(siteConfig.get(EasyMock.anyObject(Property.class))).andAnswer(() -> {
      Object[] args = EasyMock.getCurrentArguments();
      return systemConf.get((Property) args[0]);
    }).anyTimes();
    EasyMock.expect(siteConfig.getBoolean(EasyMock.anyObject(Property.class))).andAnswer(() -> {
      Object[] args = EasyMock.getCurrentArguments();
      return systemConf.getBoolean((Property) args[0]);
    }).anyTimes();

    EasyMock.expect(siteConfig.iterator()).andAnswer(() -> systemConf.iterator()).anyTimes();
    ServerContext context = createMock(ServerContext.class);
    expect(context.getServerConfFactory()).andReturn(factory).anyTimes();
    expect(context.getProperties()).andReturn(new Properties()).anyTimes();
    expect(context.getZooKeepers()).andReturn("localhost").anyTimes();
    expect(context.getInstanceName()).andReturn("test").anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(30000).anyTimes();
    expect(context.getInstanceID()).andReturn("1111").anyTimes();
    expect(context.getZooKeeperRoot()).andReturn(Constants.ZROOT + "/1111").anyTimes();

    replay(factory, siteConfig, context);

    refs = new WrappedCloseWriteAheadLogReferences(context);
  }

  @Test
  public void unclosedWalsLeaveStatusOpen() throws Exception {
    Set<String> wals = Collections.emptySet();
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(
        ReplicationSection.getRowPrefix() + "file:/accumulo/wal/tserver+port/12345");
    m.put(ReplicationSection.COLF, new Text("1"),
        StatusUtil.fileCreatedValue(System.currentTimeMillis()));
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    try (Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(ReplicationSection.COLF);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertFalse(status.getClosed());
    }
  }

  @Test
  public void closedWalsUpdateStatus() throws Exception {
    String file = "file:/accumulo/wal/tserver+port/12345";
    Set<String> wals = Collections.singleton(file);
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation m = new Mutation(ReplicationSection.getRowPrefix() + file);
    m.put(ReplicationSection.COLF, new Text("1"),
        StatusUtil.fileCreatedValue(System.currentTimeMillis()));
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    try (Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(ReplicationSection.COLF);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertTrue(status.getClosed());
    }
  }

  @Test
  public void partiallyReplicatedReferencedWalsAreNotClosed() throws Exception {
    String file = "file:/accumulo/wal/tserver+port/12345";
    Set<String> wals = Collections.singleton(file);
    BatchWriter bw = ReplicationTable.getBatchWriter(conn);
    Mutation m = new Mutation(file);
    StatusSection.add(m, Table.ID.of("1"), ProtobufUtil.toValue(StatusUtil.ingestedUntil(1000)));
    bw.addMutation(m);
    bw.close();

    refs.updateReplicationEntries(conn, wals);

    try (Scanner s = ReplicationTable.getScanner(conn)) {
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertFalse(status.getClosed());
    }
  }
}
