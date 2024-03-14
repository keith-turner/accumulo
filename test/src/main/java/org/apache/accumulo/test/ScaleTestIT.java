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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
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
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScaleTestIT extends ConfigurableMacBase {

  private static final Logger log = LoggerFactory.getLogger(ScaleTestIT.class);

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
    return Duration.ofHours(24);
  }

  @Test
  public void testManyTablets() throws Exception {

    getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
    getCluster().getClusterControl().startCompactors(Compactor.class, 10, "CG1");

    var executor = Executors.newCachedThreadPool();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs1", Property.TABLE_MAJC_RATIO.getKey(), "1");

      var splits = IntStream.range(0, 20_000).mapToObj(i -> String.format("%09d", i)).map(Text::new)
          .collect(Collectors.toCollection(TreeSet::new));

      long ct1 = System.currentTimeMillis();
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).setProperties(props));
      long ct2 = System.currentTimeMillis();

      log.info("created table with {} split in {} ms\n", splits.size(), ct2 - ct1);

      // create a subset of rows to bulk import to

      List<Future<Integer>> futures = new ArrayList<>();

      int iterations = 100;

      // run three bulk import loops that will go to the same subset of tablets, this will cause
      // lots of compactions on those tablets
      futures.add(
          executor.submit(() -> runBulk(splits, c, tableName, "A", iterations, r -> r % 100 == 0)));
      futures.add(
          executor.submit(() -> runBulk(splits, c, tableName, "B", iterations, r -> r % 100 == 0)));
      futures.add(
          executor.submit(() -> runBulk(splits, c, tableName, "C", iterations, r -> r % 100 == 0)));

      // run three bulk import loops that will go to random tablets
      Random rand = new Random();
      futures.add(executor.submit(
          () -> runBulk(splits, c, tableName, "X", iterations, r -> rand.nextInt(100) == 0)));
      futures.add(executor.submit(
          () -> runBulk(splits, c, tableName, "Y", iterations, r -> rand.nextInt(100) == 0)));
      futures.add(executor.submit(
          () -> runBulk(splits, c, tableName, "Z", iterations, r -> rand.nextInt(100) == 0)));

      long entriesWritten = futures.stream().mapToInt(future -> {
        try {
          return future.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).sum();

      long st1 = System.currentTimeMillis();
      try (var scanner = c.createBatchScanner(tableName, Authorizations.EMPTY, 16)) {
        scanner.setRanges(List.of(new Range()));
        long seen = scanner.stream().count();
        Assertions.assertEquals(entriesWritten, seen);
      }
      long st2 = System.currentTimeMillis();
      log.info("Scanned {} entries in {} ms", entriesWritten, st2 - st1);

      while (true) {
        // give compactions a chance to run
        UtilWaitThread.sleep(10000);
        Map<Integer,Integer> histogram = new HashMap<>();
        AtomicBoolean sawCompactions = new AtomicBoolean(false);
        var stats = getServerContext().getAmple().readTablets()
            .forTable(getServerContext().getTableId(tableName)).build().stream().peek(tm -> {
              if (tm.getExternalCompactions().size() > 0) {
                sawCompactions.set(true);
              }
            }).mapToInt(tm -> tm.getFiles().size()).peek(i -> histogram.merge(i, 1, Integer::sum))
            .summaryStatistics();
        log.info("File per tablet stats : {} ", stats);
        log.info("File per tablet histogram : {} ", histogram);
        if (!sawCompactions.get()) {
          break;
        }
      }

      // command to run to count the external compactions that ran
      // grep "Returning external job" CompactionCoordinator_* | wc
    }
  }

  private int runBulk(SortedSet<Text> rows, AccumuloClient c, String tableName, String prefix,
      int iterations, IntPredicate rowPredicate) {
    var testRows = rows.stream().filter(s -> rowPredicate.test(Integer.parseInt(s.toString())))
        .collect(Collectors.toList());

    for (int i = 0; i < iterations; i++) {
      var bulkDir =
          new Path(getCluster().getTemporaryPath(), String.format("bulk-%s%06d", prefix, i));
      try {
        var qual = new Text(prefix + i);
        testRows.forEach(row -> {
          try (
              var out =
                  getCluster().getFileSystem().create(new Path(bulkDir, row.toString() + ".rf"));
              var writer = RFile.newWriter().to(out).build();) {
            writer.startDefaultLocalityGroup();
            writer.append(new Key(row, new Text("unique"), qual), new Value("V" + row));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

        long t1 = System.currentTimeMillis();
        c.tableOperations().importDirectory(bulkDir.toString()).to(tableName).load();
        long t2 = System.currentTimeMillis();

        log.info("{} imported {} files in {} ms", prefix, testRows.size(), t2 - t1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        try {
          getCluster().getFileSystem().delete(bulkDir, true);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

    }

    return testRows.size() * iterations;
  }
}
