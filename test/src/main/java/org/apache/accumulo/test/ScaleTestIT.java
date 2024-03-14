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

import java.time.Duration;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ScaleTestIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setNumTservers(1);
    cfg.setMemory(ServerType.TABLET_SERVER, 8, MemoryUnit.GIGABYTE);

    cfg.setProperty("tserver.compaction.major.service.cs1.planner",
        DefaultCompactionPlanner.class.getName());
    cfg.setProperty("tserver.compaction.major.service.cs1.planner.opts.executors",
        "[{'name':'all', 'type': 'external', 'queue': 'CG1'}]");
    cfg.setNumCompactors(10);
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT.getKey(), "20");
  }

  protected Duration defaultTimeout() {
    return Duration.ofMinutes(30);
  }

  @Test
  public void testManyTablets() throws Exception {

    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
    getCluster().getClusterControl().startCompactors(Compactor.class, 10, "CG1");

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs1", Property.TABLE_MAJC_RATIO.getKey(), "1");

      var splits = IntStream.range(0, 20_000).mapToObj(i -> String.format("%09d", i))
          .map(s -> new Text(s)).collect(Collectors.toCollection(() -> new TreeSet<Text>()));

      long ct1 = System.currentTimeMillis();
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).setProperties(props));
      long ct2 = System.currentTimeMillis();

      System.out.printf("created table with %d split in %d ms\n", splits.size(), ct2 - ct1);

      // create a subset of rows to bulk import to
      var testRows = splits.stream().filter(s -> Integer.parseInt(s.toString()) % 100 == 0)
              .collect(Collectors.toList());

      for (int i = 0; i < 1000; i++) {
        var bulkDir = new Path(getCluster().getTemporaryPath(), String.format("bulk-%06d", i));
        var qual = new Text(""+i);
        testRows.forEach(row -> {
          try (
              var out =
                  getCluster().getFileSystem().create(new Path(bulkDir, row.toString() + ".rf"));
              var writer = RFile.newWriter().to(out).build();) {
            writer.startDefaultLocalityGroup();
            writer.append(new Key(row, new Text("iteration"), qual), new Value("V" + row));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        long t1 = System.currentTimeMillis();
        c.tableOperations().importDirectory(bulkDir.toString()).to(tableName).load();
        long t2 = System.currentTimeMillis();

        System.out.printf("imported %d files in %d ms\n", testRows.size(), t2 - t1);
      }

      // TODO verify data
    }
  }
}
