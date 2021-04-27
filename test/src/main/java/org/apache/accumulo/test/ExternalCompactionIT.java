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
package org.apache.accumulo.test;

import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.compactor.CompactionEnvironment.CompactorIterEnv;
import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.coordinator.ExternalCompactionMetrics;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl.ProcessInfo;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

public class ExternalCompactionIT extends ConfigurableMacBase {

  private static final Logger LOG = LoggerFactory.getLogger(ExternalCompactionIT.class);

  private static final int MAX_DATA = 1000;

  private static String row(int r) {
    return String.format("r:%04d", r);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty("tserver.compaction.major.service.cs1.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs1.planner.opts.executors",
        "[{'name':'all','externalQueue':'DCQ1'}]");
    cfg.setProperty("tserver.compaction.major.service.cs2.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs2.planner.opts.executors",
        "[{'name':'all','externalQueue':'DCQ2'}]");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  public static class TestFilter extends Filter {

    int modulus = 1;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      // this cast should fail if the compaction is running in the tserver
      CompactorIterEnv cienv = (CompactorIterEnv) env;

      Preconditions.checkArgument(!cienv.getQueueName().isEmpty());
      Preconditions
          .checkArgument(options.getOrDefault("expectedQ", "").equals(cienv.getQueueName()));

      Preconditions.checkArgument(cienv.isFullMajorCompaction());
      Preconditions.checkArgument(cienv.isUserCompaction());
      Preconditions.checkArgument(cienv.getIteratorScope() == IteratorScope.majc);
      Preconditions.checkArgument(!cienv.isSamplingEnabled());

      // if the init function is never called at all, then not setting the modulus option should
      // cause the test to fail
      if (options.containsKey("modulus")) {
        modulus = Integer.parseInt(options.get("modulus"));
      }
    }

    @Override
    public boolean accept(Key k, Value v) {
      return Integer.parseInt(v.toString()) % modulus == 0;
    }

  }

  @Test
  public void testExternalCompaction() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      String table1 = "ectt1";
      createTable(client, table1, "cs1");

      String table2 = "ectt2";
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(Compactor.class, "-q", "DCQ2");
      cluster.exec(CompactionCoordinator.class);

      compact(client, table1, 2, "DCQ1", true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, "DCQ2", true);
      verify(client, table2, 3);

    }
  }

  @Test
  public void testSplitDuringExternalCompaction() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String table1 = "ectt6";
      createTable(client, table1, "cs1");
      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);
      writeData(client, table1);

      cluster.exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      cluster.exec(TestCompactionCoordinator.class);
      compact(client, table1, 2, "DCQ1", false);

      // Wait for the compaction to start by waiting for 1 external compaction column
      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = null;
      do {
        if (null != tm) {
          tm.close();
        }
        tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
      } while (md.size() == 0);
      tm.close();
      md.clear();

      // ExternalDoNothingCompactor will not compact, it will wait, split the table.
      SortedSet<Text> splits = new TreeSet<>();
      int jump = MAX_DATA / 5;
      for (int r = jump; r < MAX_DATA; r += jump) {
        splits.add(new Text(row(r)));
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().addSplits(table1, splits);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      // Check that there is one failed compaction in the coordinator metrics
      assertEquals(1, metrics.getStarted());
      assertEquals(1, metrics.getRunning()); // CBUG: Should be zero when #2032 is resolved
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());

    }

  }

  @Test
  public void testMergeDuringExternalCompaction() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String table1 = "ectt7";
      SortedSet<Text> splits = new TreeSet<>();
      int jump = MAX_DATA / 2;
      for (int r = jump; r < MAX_DATA; r += jump) {
        splits.add(new Text(row(r)));
      }
      createTable(client, table1, "cs1", splits);
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks merge
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      TableId tid = Tables.getTableId(getCluster().getServerContext(), table1);

      cluster.exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      cluster.exec(TestCompactionCoordinator.class);

      // Wait for the compaction to start by waiting for 1 external compaction column
      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = null;
      do {
        if (null != tm) {
          tm.close();
        }
        UtilWaitThread.sleep(50);
        tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
      } while (md.size() == 0);
      tm.close();

      md.clear();
      tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.PREV_ROW).build();
      tm.forEach(t -> md.add(t));
      assertEquals(2, md.size());
      Text start = md.get(0).getPrevEndRow();
      Text end = md.get(1).getEndRow();

      assertEquals(0, getCoordinatorMetrics().getFailed());

      // Merge - blocking operation
      client.tableOperations().merge(table1, start, end);

      // Confirm that there are no external compaction markers or final states in the metadata table
      md.clear();
      tm = getCluster().getServerContext().getAmple().readTablets().forTable(tid)
          .fetch(ColumnType.ECOMP).build();
      tm.forEach(t -> md.add(t));
      assertEquals(0, md.size());
      assertEquals(0,
          getCluster().getServerContext().getAmple().getExternalCompactionFinalStates().count());

      // Wait for the table to merge by waiting for only 1 tablet to show up in the metadata table
      tm.close();

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      // Check that there is one failed compaction in the coordinator metrics
      assertTrue(metrics.getStarted() > 0);
      assertTrue(metrics.getRunning() > 0); // CBUG: Should be zero when #2032 is resolved
      assertEquals(0, metrics.getCompleted());
      assertTrue(metrics.getFailed() > 0);

    }

  }

  @Test
  public void testManytablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String table1 = "ectt4";

      SortedSet<Text> splits = new TreeSet<>();
      int jump = MAX_DATA / 200;

      for (int r = jump; r < MAX_DATA; r += jump) {
        splits.add(new Text(row(r)));
      }

      createTable(client, table1, "cs1", splits);

      writeData(client, table1);

      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(CompactionCoordinator.class);

      compact(client, table1, 3, "DCQ1", true);

      verify(client, table1, 3);
    }
  }

  @Test
  public void testUserCompactionCancellation() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      String table1 = "ectt6";
      createTable(client, table1, "cs1");
      writeData(client, table1);

      // The ExternalDoNothingCompactor creates a compaction thread that
      // sleeps for 5 minutes.
      // Wait for the coordinator to insert the running compaction metadata
      // entry into the metadata table, then cancel the compaction
      cluster.exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      cluster.exec(TestCompactionCoordinator.class);

      compact(client, table1, 2, "DCQ1", false);

      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forLevel(DataLevel.USER).fetch(ColumnType.ECOMP).build();
      tm.forEach(t -> md.add(t));

      while (md.size() == 0) {
        tm.close();
        tm = getCluster().getServerContext().getAmple().readTablets().forLevel(DataLevel.USER)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().cancelCompaction(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      assertEquals(1, metrics.getStarted());
      assertEquals(1, metrics.getRunning()); // CBUG: Should be zero when #2032 is resolved
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());
    }
  }

  @Test
  public void testDeleteTableDuringExternalCompaction() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      String table1 = "ectt5";
      createTable(client, table1, "cs1");
      // set compaction ratio to 1 so that majc occurs naturally, not user compaction
      // user compaction blocks delete
      client.tableOperations().setProperty(table1, Property.TABLE_MAJC_RATIO.toString(), "1.0");
      // cause multiple rfiles to be created
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);
      writeData(client, table1);

      // The ExternalDoNothingCompactor creates a compaction thread that
      // sleeps for 5 minutes. The compaction should occur naturally.
      // Wait for the coordinator to insert the running compaction metadata
      // entry into the metadata table, then delete the table.
      cluster.exec(ExternalDoNothingCompactor.class, "-q", "DCQ1");
      cluster.exec(TestCompactionCoordinator.class);

      List<TabletMetadata> md = new ArrayList<>();
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forLevel(DataLevel.USER).fetch(ColumnType.ECOMP).build();
      tm.forEach(t -> md.add(t));

      while (md.size() == 0) {
        tm.close();
        tm = getCluster().getServerContext().getAmple().readTablets().forLevel(DataLevel.USER)
            .fetch(ColumnType.ECOMP).build();
        tm.forEach(t -> md.add(t));
      }

      assertEquals(0, getCoordinatorMetrics().getFailed());

      client.tableOperations().delete(table1);

      // wait for failure or test timeout
      ExternalCompactionMetrics metrics = getCoordinatorMetrics();
      while (metrics.getFailed() == 0) {
        UtilWaitThread.sleep(250);
        metrics = getCoordinatorMetrics();
      }

      tm = getCluster().getServerContext().getAmple().readTablets().forLevel(DataLevel.USER)
          .fetch(ColumnType.ECOMP).build();
      assertEquals(0, tm.stream().count());
      tm.close();

      // The metadata tablets will be deleted from the metadata table because we have deleted the
      // table. Verify that the compaction failed by looking at the metrics in the Coordinator.
      assertEquals(1, metrics.getStarted());
      assertEquals(1, metrics.getRunning()); // CBUG: Should be zero when #2032 is resolved
      assertEquals(0, metrics.getCompleted());
      assertEquals(1, metrics.getFailed());
    }
  }

  // CBUG add test that configures output file for external compaction

  // CBUG add test that verifies iterators configured on table (not on user compaction) are used in
  // external compaction

  @Test
  public void testExternalCompactionDeadTServer() throws Exception {
    // Shut down the normal TServers
    getCluster().getProcesses().get(TABLET_SERVER).forEach(p -> {
      try {
        getCluster().killProcess(TABLET_SERVER, p);
      } catch (Exception e) {
        fail("Failed to shutdown tablet server");
      }
    });
    // Start our TServer that will not commit the compaction
    ProcessInfo process = cluster.exec(ExternalCompactionTServer.class);

    final String table3 = "ectt3";
    try (final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      createTable(client, table3, "cs1");
      writeData(client, table3);
      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(CompactionCoordinator.class);
      compact(client, table3, 2, "DCQ1", false);

      // ExternalCompactionTServer will not commit the compaction. Wait for the
      // metadata table entries to show up.
      LOG.info("Waiting for external compaction to complete.");
      Stream<ExternalCompactionFinalState> fs =
          getCluster().getServerContext().getAmple().getExternalCompactionFinalStates();
      while (fs.count() == 0) {
        LOG.info("Waiting for compaction completed marker to appear");
        UtilWaitThread.sleep(1000);
        fs = getCluster().getServerContext().getAmple().getExternalCompactionFinalStates();
      }

      LOG.info("Validating metadata table contents.");
      TabletsMetadata tm = getCluster().getServerContext().getAmple().readTablets()
          .forLevel(DataLevel.USER).fetch(ColumnType.ECOMP).build();
      List<TabletMetadata> md = new ArrayList<>();
      tm.forEach(t -> md.add(t));
      assertEquals(1, md.size());
      TabletMetadata m = md.get(0);
      Map<ExternalCompactionId,ExternalCompactionMetadata> em = m.getExternalCompactions();
      assertEquals(1, em.size());
      List<ExternalCompactionFinalState> finished = new ArrayList<>();
      getCluster().getServerContext().getAmple().getExternalCompactionFinalStates()
          .forEach(f -> finished.add(f));
      assertEquals(1, finished.size());
      assertEquals(em.entrySet().iterator().next().getKey(),
          finished.get(0).getExternalCompactionId());
      tm.close();

      // Force a flush on the metadata table before killing our tserver
      client.tableOperations().compact("accumulo.metadata", new CompactionConfig().setWait(true));
    }

    // Stop our TabletServer. Need to perform a normal shutdown so that the WAL is closed normally.
    LOG.info("Stopping our tablet server");
    Process tsp = process.getProcess();
    if (tsp.supportsNormalTermination()) {
      cluster.stopProcessWithTimeout(tsp, 60, TimeUnit.SECONDS);
    } else {
      LOG.info("Stopping tserver manually");
      new ProcessBuilder("kill", Long.toString(tsp.pid())).start();
      tsp.waitFor();
    }

    // Start a TabletServer to commit the compaction.
    LOG.info("Starting normal tablet server");
    getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

    // Wait for the compaction to be committed.
    LOG.info("Waiting for compaction completed marker to disappear");
    Stream<ExternalCompactionFinalState> fs =
        getCluster().getServerContext().getAmple().getExternalCompactionFinalStates();
    while (fs.count() != 0) {
      LOG.info("Waiting for compaction completed marker to disappear");
      UtilWaitThread.sleep(100);
      fs = getCluster().getServerContext().getAmple().getExternalCompactionFinalStates();
    }
    try (final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      verify(client, table3, 2);
    }
  }

  private ExternalCompactionMetrics getCoordinatorMetrics() throws Exception {
    HttpRequest req =
        HttpRequest.newBuilder().GET().uri(new URI("http://localhost:9099/metrics")).build();
    HttpClient hc =
        HttpClient.newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL).build();
    HttpResponse<String> res = hc.send(req, BodyHandlers.ofString());
    assertEquals(200, res.statusCode());
    String metrics = res.body();
    assertNotNull(metrics);
    System.out.println("Metrics response: " + metrics);
    return new Gson().fromJson(metrics, ExternalCompactionMetrics.class);
  }

  private void verify(AccumuloClient client, String table1, int modulus)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try (Scanner scanner = client.createScanner(table1)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertTrue(Integer.parseInt(entry.getValue().toString()) % modulus == 0);
        count++;
      }

      int expectedCount = 0;
      for (int i = 0; i < MAX_DATA; i++) {
        if (i % modulus == 0)
          expectedCount++;
      }

      assertEquals(expectedCount, count);
    }
  }

  private void compact(final AccumuloClient client, String table1, int modulus,
      String expectedQueue, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
    // make sure iterator options make it to compactor process
    iterSetting.addOption("expectedQ", expectedQueue);
    iterSetting.addOption("modulus", modulus + "");
    CompactionConfig config =
        new CompactionConfig().setIterators(List.of(iterSetting)).setWait(wait);
    client.tableOperations().compact(table1, config);
  }

  private void createTable(AccumuloClient client, String tableName, String service)
      throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);

    client.tableOperations().create(tableName, ntc);

  }

  private void createTable(AccumuloClient client, String tableName, String service,
      SortedSet<Text> splits) throws Exception {
    Map<String,String> props =
        Map.of("table.compaction.dispatcher", SimpleCompactionDispatcher.class.getName(),
            "table.compaction.dispatcher.opts.service", service);
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props).withSplits(splits);

    client.tableOperations().create(tableName, ntc);

  }

  private void writeData(AccumuloClient client, String table1) throws MutationsRejectedException,
      TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < MAX_DATA; i++) {
        Mutation m = new Mutation(row(i));
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }
}
