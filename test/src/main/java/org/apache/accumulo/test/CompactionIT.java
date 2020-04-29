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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompactionIT extends SharedMiniClusterBase {

  public static class TestPlanner implements CompactionPlanner {

    private int filesPerExecutor;
    private List<CompactionExecutorId> executorIds;

    @Override
    public void init(InitParameters params) {
      var executors = Integer.parseInt(params.getOptions().get("executors"));
      this.filesPerExecutor = Integer.parseInt(params.getOptions().get("filesPerExecutor"));

      this.executorIds = new ArrayList<>();

      for (int i = 0; i < executors; i++) {
        var ceid = params.getExecutorManager().createExecutor("e" + i, 2);
        executorIds.add(ceid);
      }

    }

    @Override
    public CompactionPlan makePlan(PlanningParameters params) {
      if (params.getKind() == CompactionKind.SYSTEM) {
        var planBuilder = params.createPlanBuilder();

        if (params.getCandidates().size() > filesPerExecutor * executorIds.size()) {
          ArrayList<CompactableFile> files = new ArrayList<>(params.getCandidates());
          for (CompactionExecutorId ceid : executorIds) {
            var group = files.subList(files.size() - filesPerExecutor, files.size());
            planBuilder.addJob(1, ceid, group);
            group.clear();
          }
        }
        return planBuilder.build();
      } else {
        return params.createPlanBuilder().addJob(1, executorIds.get(0), params.getCandidates())
            .build();
      }
    }

  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig((miniCfg, coreSite) -> {
      Map<String,String> siteCfg = new HashMap<>();

      var csp = Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey();
      siteCfg.put(csp + "cs1.planner", TestPlanner.class.getName());
      siteCfg.put(csp + "cs1.planner.opts.executors", "3");
      siteCfg.put(csp + "cs1.planner.opts.filesPerExecutor", "5");

      siteCfg.put(csp + "cs2.planner", TestPlanner.class.getName());
      siteCfg.put(csp + "cs2.planner.opts.executors", "5");
      siteCfg.put(csp + "cs2.planner.opts.filesPerExecutor", "2");

      miniCfg.setSiteConfig(siteCfg);
    });
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testTest() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      createTable(client, "tab1", "cs1");
      createTable(client, "tab2", "cs2");

      addFiles(client, "tab1", 14);
      addFiles(client, "tab2", 9);

      Assert.assertEquals(14, getFiles(client, "tab1").size());
      Assert.assertEquals(9, getFiles(client, "tab2").size());

      addFiles(client, "tab1", 1);
      addFiles(client, "tab2", 1);

      while (getFiles(client, "tab1").size() > 3 || getFiles(client, "tab2").size() > 5) {
        Thread.sleep(100);
      }

      Assert.assertEquals(3, getFiles(client, "tab1").size());
      Assert.assertEquals(5, getFiles(client, "tab2").size());
    }
  }

  private Set<String> getFiles(AccumuloClient client, String name) {
    var tableId = TableId.of(client.tableOperations().tableIdMap().get(name));

    try (var tabletsMeta =
        TabletsMetadata.builder().forTable(tableId).fetch(ColumnType.FILES).build(client)) {
      return tabletsMeta.stream().flatMap(tm -> tm.getFiles().stream())
          .map(StoredTabletFile::getFileName).collect(Collectors.toSet());
    }
  }

  private void addFiles(AccumuloClient client, String table, int num) throws Exception {
    try (var writer = client.createBatchWriter(table)) {
      for (int i = 0; i < num; i++) {
        Mutation mut = new Mutation("r" + i);
        mut.put("f1", "q1", "v" + i);
        writer.addMutation(mut);
        writer.flush();

        client.tableOperations().flush(table, null, null, true);
      }
    }
  }

  private void createTable(AccumuloClient client, String name, String compactionService)
      throws Exception {
    NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
        Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", compactionService));
    client.tableOperations().create(name, ntc);
  }
}
