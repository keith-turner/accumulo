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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.compactor.CompactionEnvironment.CompactorIterEnv;
import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;

public class ExternalCompactionIT extends ConfigurableMacBase {

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
  }

  public static class TestFilter extends Filter {

    int modulus = 1;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

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

      wrtieData(client, table1);
      wrtieData(client, table2);

      cluster.exec(Compactor.class, "-q", "DCQ1");
      cluster.exec(Compactor.class, "-q", "DCQ2");
      cluster.exec(CompactionCoordinator.class);

      compact(client, table1, 2, "DCQ1");
      verify(client, table1, 2);

      compact(client, table2, 3, "DCQ2");
      verify(client, table2, 3);

    }
  }

  private void verify(AccumuloClient client, String table1, int modulus)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    try (Scanner scanner = client.createScanner(table1)) {
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        Assert.assertTrue(Integer.parseInt(entry.getValue().toString()) % modulus == 0);
        count++;
      }

      int expectedCount = 0;
      for (int i = 0; i < 10; i++) {
        if (i % modulus == 0)
          expectedCount++;
      }

      Assert.assertEquals(expectedCount, count);
    }
  }

  private void compact(AccumuloClient client, String table1, int modulus, String expectedQueue)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
    // make sure iterator options make it to compactor process
    iterSetting.addOption("expectedQ", expectedQueue);
    iterSetting.addOption("modulus", modulus + "");
    CompactionConfig config =
        new CompactionConfig().setIterators(List.of(iterSetting)).setWait(true);
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

  private void wrtieData(AccumuloClient client, String table1) throws MutationsRejectedException,
      TableNotFoundException, AccumuloException, AccumuloSecurityException {
    try (BatchWriter bw = client.createBatchWriter(table1)) {
      for (int i = 0; i < 10; i++) {
        Mutation m = new Mutation("r:" + i);
        m.put("", "", "" + i);
        bw.addMutation(m);
      }
    }

    client.tableOperations().flush(table1);
  }
}
