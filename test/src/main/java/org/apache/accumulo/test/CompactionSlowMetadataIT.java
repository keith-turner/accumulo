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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Test;

public class CompactionSlowMetadataIT extends AccumuloClusterHarness {

  public static class TestCompactionPlanner implements CompactionPlanner {

    private CompactionExecutorId execId;

    @Override
    public void init(InitParameters params) {
      this.execId = params.getExecutorManager().createExecutor("all",40);
    }

    @Override
    public CompactionPlan makePlan(PlanningParameters params) {
      if(params.getCandidates().size()>1 && params.getKind() == CompactionKind.SYSTEM){
        return params.createPlanBuilder().addJob((short)1, execId, params.getCandidates()).build();
      }

      return params.createPlanBuilder().build();
    }
  }



  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "1s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.setProperty(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey()+"test.planner",TestCompactionPlanner.class.getName());
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(300);
  }

  public static class SleepyConstraint implements Constraint {

    private static final SecureRandom rand = new SecureRandom();

    @Override
    public String getViolationDescription(short violationCode) {
      return "No such violation";
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {

      var numFiles = mutation.getUpdates().stream().filter(columnUpdate -> new String(columnUpdate.getColumnFamily(), UTF_8).equals("file")).count();
      var hasDeletes = mutation.getUpdates().stream().anyMatch(ColumnUpdate::isDeleted);

      if (hasDeletes && numFiles > 1) {
        // this is probably a compaction mutation, so lets hold it up
        UtilWaitThread.sleep(1000);
      }

      return null;
    }
  }

  @Test
  public void testCompactions() throws Exception {
    String[] tables = getUniqueNames(10);

    var executor = Executors.newCachedThreadPool();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      c.tableOperations().addConstraint("accumulo.metadata", SleepyConstraint.class.getName());

      UtilWaitThread.sleep(1000);

      List<Future<?>> futures = new ArrayList<>();

      for (String table : tables) {
        futures.add(executor.submit(createWriteTask(table, 100, c)));
      }

      int errorCount = 0;

      // wait for all futures to complete
      for (var future : futures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          errorCount++;
        }
      }

      assertEquals(0, errorCount);

      for (String table : tables) {
        try (Scanner scanner = c.createScanner(table)) {
          assertEquals(0, scanner.stream().count());
        }
      }

    } finally {
      executor.shutdownNow();
    }
  }

  private static Callable<Void> createWriteTask(String table, int iterations, AccumuloClient c) {
    return () -> {

      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
              Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "test"));

      c.tableOperations().create(table, ntc);

      for (int row = 0; row < iterations; row++) {

        try (BatchWriter writer = c.createBatchWriter(table)) {
          Mutation m = new Mutation("r" + row);
          m.put("f1", "q1", new Value("v1"));
          m.put("f1", "q2", new Value("v2"));
          writer.addMutation(m);
        }

        c.tableOperations().flush(table, null, null, true);

        try (BatchWriter writer = c.createBatchWriter(table)) {
          Mutation m = new Mutation("r" + row);
          m.putDelete("f1", "q1");
          writer.addMutation(m);
        }

        c.tableOperations().flush(table, null, null, true);
      }
      return null;
    };
  }
}
