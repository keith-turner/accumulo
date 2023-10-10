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
import static java.util.stream.Collectors.toMap;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.CountingSummarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.data.constraints.DefaultKeySizeConstraint;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The purpose of this test is to exercise a large amount of Accumulo's features in a single test.
 * It provides a quick test that verifies a lot of functionality is working for basic use. This test
 * does not provide deep coverage of the features it tests.
 */
@Tag(SUNNY_DAY)
public class ComprehensiveIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.securityOperations().changeUserAuthorizations("root",
          new Authorizations("CAT", "DOG"));
    }
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testBulkImport() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      bulkImport(client, table,
          List.of(generateKeys(0, 100, tr -> true), generateKeys(100, 200, tr -> true)));

      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 200, tr -> true));

      bulkImport(client, table, List.of(generateKeys(200, 300, tr -> true),
          generateKeys(300, 400, tr -> true), generateKeys(400, 500, tr -> true)));

      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 500, tr -> true));
    }
  }

  @Test
  public void testMergeAndSplit() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      write(client, table, generateMutations(0, 300, tr -> true));

      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 300, tr -> true));

      // test adding splits to a table
      var splits = new TreeSet<>(List.of(new Text(row(75)), new Text(row(150))));
      client.tableOperations().addSplits(table, splits);
      assertEquals(splits, new TreeSet<>(client.tableOperations().listSplits(table)));
      // adding splits should not change data
      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 300, tr -> true));

      // test merging splits away
      client.tableOperations().merge(table, null, null);
      assertEquals(Set.of(), new TreeSet<>(client.tableOperations().listSplits(table)));
      // merging should not change data
      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 300, tr -> true));

      splits = new TreeSet<>(List.of(new Text(row(33)), new Text(row(66))));
      client.tableOperations().addSplits(table, splits);
      assertEquals(splits, new TreeSet<>(client.tableOperations().listSplits(table)));
      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 300, tr -> true));

      client.tableOperations().merge(table, null, null);
      assertEquals(Set.of(), new TreeSet<>(client.tableOperations().listSplits(table)));
      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 300, tr -> true));

    }
  }

  @Test
  public void testFlushAndIterator() throws Exception {
    String table = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      // test attaching an iterator to a table AND flushing a table
      client.tableOperations().attachIterator(table,
          new IteratorSetting(100, "fam3", FamThreeFilter.class),
          EnumSet.of(IteratorUtil.IteratorScope.minc));
      write(client, table, generateMutations(300, 310, tr -> true));
      // prior to flushing fam3 should not be filtered by the attached iterator
      verifyData(client, table, new Authorizations("CAT", "DOG"),
          generateKeys(300, 310, tr -> true));
      client.tableOperations().flush(table, null, null, true);
      // the attached iterator should be applied when the flush happens filtering out family 3.
      // TODO its possible the iterator setting did not make it to the tserver
      verifyData(client, table, new Authorizations("CAT", "DOG"),
          generateKeys(300, 310, tr -> tr.row < 300 || tr.fam != 3));
    }
  }

  @Test
  public void testConstraint() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      // test adding and removing a constraint, do this before taking table online offline so that
      // test can be sure constraint is picked up
      assertEquals(Map.of(DefaultKeySizeConstraint.class.getName(), 1),
          client.tableOperations().listConstraints(table));
      client.tableOperations().addConstraint(table, TestConstraint.class.getName());
      assertEquals(
          Map.of(DefaultKeySizeConstraint.class.getName(), 1, TestConstraint.class.getName(), 2),
          client.tableOperations().listConstraints(table));

      assertTrue(client.tableOperations().isOnline(table));

      // test taking table offline and then back online
      client.tableOperations().offline(table, true);
      Assertions.assertThrows(TableOfflineException.class, () -> {
        try (var scanner = client.createScanner(table)) {
          assertEquals(0, scanner.stream().count());
        }
      });
      assertFalse(client.tableOperations().isOnline(table));
      client.tableOperations().online(table, true);

      // continue testing constraint, it should cause a write to fail
      var mre = assertThrows(MutationsRejectedException.class, () -> {
        try (var writer = client.createBatchWriter(table)) {
          Mutation m = new Mutation("not a number");
          m.put("5", "5", "5");
          writer.addMutation(m);
        }
      });

      assertEquals(1, mre.getConstraintViolationSummaries().size());
      assertEquals("No numeric field seen",
          mre.getConstraintViolationSummaries().get(0).getViolationDescription());

      // ensure no data was added to table, constraint supposedly prevented previous write
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      client.tableOperations().removeConstraint(table, 2);
      assertEquals(Map.of(DefaultKeySizeConstraint.class.getName(), 1),
          client.tableOperations().listConstraints(table));

      client.tableOperations().offline(table, true);
      client.tableOperations().online(table, true);

      try (var writer = client.createBatchWriter(table)) {
        Mutation m = new Mutation("not a number");
        m.put("5", "5", "5");
        writer.addMutation(m);
      }

      try (var scanner = client.createScanner(table)) {
        assertEquals(1, scanner.stream().count());
      }

    }
  }

  /*
   * Other tests in this IT show that tservers react to changes in table props, so not testing that
   * here. Just focusing on the table property APIs.
   */
  @Test
  public void testTableProperties() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table, new NewTableConfiguration().withoutDefaultIterators());

      assertEquals(Map.of(), client.tableOperations().getTableProperties(table));

      client.tableOperations().setProperty(table, "table.custom.compit", "123");
      assertEquals(Map.of("table.custom.compit", "123"),
          client.tableOperations().getTableProperties(table));
      assertTrue(
          client.tableOperations().getConfiguration(table).containsKey("table.custom.compit"));

      client.tableOperations().setProperty(table, "table.custom.compit2", "abc");
      assertEquals(Map.of("table.custom.compit", "123", "table.custom.compit2", "abc"),
          client.tableOperations().getTableProperties(table));
      assertTrue(client.tableOperations().getConfiguration(table).keySet()
          .containsAll(Set.of("table.custom.compit", "table.custom.compit2")));

      client.tableOperations().removeProperty(table, "table.custom.compit");
      assertEquals(Map.of("table.custom.compit2", "abc"),
          client.tableOperations().getTableProperties(table));
      assertTrue(
          client.tableOperations().getConfiguration(table).containsKey("table.custom.compit2"));
    }
  }

  @Test
  public void testConditionalWriter() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      // tests conditional writer
      try (var condWriter = client.createConditionalWriter(table)) {
        try (var scanner = client.createScanner(table)) {
          scanner.setRange(new Range(row(5)));
          scanner.fetchColumn("f1", "q1");
          assertEquals(0, scanner.stream().count());
        }

        var cMutation = new ConditionalMutation(row(5));
        cMutation.addCondition(new Condition("f1", "q1").setValue("v1"));
        cMutation.put("f1", "q1", "v2");
        assertEquals(ConditionalWriter.Status.REJECTED, condWriter.write(cMutation).getStatus());

        try (var scanner = client.createScanner(table)) {
          scanner.setRange(new Range(row(5)));
          scanner.fetchColumn("f1", "q1");
          assertEquals(0, scanner.stream().count());
        }

        cMutation = new ConditionalMutation(row(5));
        cMutation.addCondition(new Condition("f1", "q1"));
        cMutation.put("f1", "q1", "v1");
        assertEquals(ConditionalWriter.Status.ACCEPTED, condWriter.write(cMutation).getStatus());

        // ensure table was changed
        try (var scanner = client.createScanner(table)) {
          scanner.setRange(new Range(row(5)));
          // tests scanner method to fetch a column family and qualifier
          scanner.fetchColumn("f1", "q1");
          assertEquals(1, scanner.stream().count());
        }

        cMutation = new ConditionalMutation(row(5));
        cMutation.addCondition(new Condition("f1", "q1").setValue("v1"));
        cMutation.putDelete("f1", "q1");
        assertEquals(ConditionalWriter.Status.ACCEPTED, condWriter.write(cMutation).getStatus());

        // ensure table was changed
        try (var scanner = client.createScanner(table)) {
          scanner.setRange(new Range(row(5)));
          scanner.fetchColumn("f1", "q1");
          assertEquals(0, scanner.stream().count());
        }
      }
    }
  }

  @Test
  public void testOverwriteAndBatchDeleter() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      write(client, table, generateMutations(0, 100, tr -> true));

      verifyData(client, table, new Authorizations("CAT", "DOG"), generateKeys(0, 100, tr -> true));

      write(client, table, generateMutations(0, 50, 0x7abc1234, tr -> true));

      // test the test
      assertNotEquals(generateKeys(0, 50, 0x7abc1234, tr -> true), generateKeys(0, 50, tr -> true));

      TreeMap<Key,Value> expected = new TreeMap<>();
      expected.putAll(generateKeys(0, 50, 0x7abc1234, tr -> true));
      expected.putAll(generateKeys(50, 100, tr -> true));

      verifyData(client, table, new Authorizations("CAT", "DOG"), expected);

      bulkImport(client, table, List.of(generateKeys(25, 75, 0x12345678, tr -> true),
          generateKeys(90, 200, 0x76543210, tr -> true)));

      expected.putAll(generateKeys(25, 75, 0x12345678, tr -> true));
      expected.putAll(generateKeys(90, 200, 0x76543210, tr -> true));

      verifyData(client, table, new Authorizations("CAT", "DOG"), expected);

      Range delRange1 = new Range(row(20), row(59));
      Range delRange2 = new Range(row(65), row(91));

      try (var deleter = client.createBatchDeleter(table, new Authorizations("CAT", "DOG"), 3)) {
        deleter.setRanges(List.of(delRange1, delRange2));
        deleter.delete();
      }

      int sizeBefore = expected.size();

      expected.keySet().removeIf(k -> delRange1.contains(k) || delRange2.contains(k));

      assertTrue(sizeBefore > expected.size());

      verifyData(client, table, new Authorizations("CAT", "DOG"), expected);

      client.tableOperations().compact(table, new CompactionConfig().setWait(true));

      verifyData(client, table, new Authorizations("CAT", "DOG"), expected);
    }
  }

  /*
   * This test happens to cover a lot features in the Accumulo public API like sampling,
   * summarizations, and some table operations. Those features do not need to be tested elsewhere.
   */
  @Test
  public void testCreateTableWithManyOptions() throws Exception {
    String[] tables = getUniqueNames(3);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      var everythingTable = tables[0];
      var everythingClone = tables[1];
      var everythingImport = tables[1];

      // create a table with a lot of initial config
      client.tableOperations().create(everythingTable, getEverythingTableConfig());

      write(client, everythingTable, generateMutations(0, 100, tr -> true));

      verifyEverythingTable(client, everythingTable);

      // test cloning a table as part of this test because the table has lots of customizations.
      client.tableOperations().clone(everythingTable, everythingClone,
          CloneConfiguration.builder().setFlush(true).build());

      // check the clone has all the same config and data as the original table
      verifyEverythingTable(client, everythingClone);

      // test compaction with an iterator that filters out col fam 3
      CompactionConfig compactionConfig = new CompactionConfig();
      compactionConfig.setIterators(List.of(new IteratorSetting(200, FamThreeFilter.class)))
          .setWait(true);
      client.tableOperations().compact(everythingClone, compactionConfig);
      // scans should not see col fam 3 now
      verifyData(client, everythingClone, new Authorizations("CAT", "DOG"),
          generateKeys(0, 100, tr -> tr.fam != 3 && tr.fam != 9));
      verifyData(client, everythingClone, new Authorizations("CAT", "DOG"),
          generateKeys(3, 4, tr -> tr.fam != 3 && tr.fam != 9),
          scanner -> scanner.setSamplerConfiguration(everythingSampleConfig));

      // remove the iterator that is suppressing col fam 9
      client.tableOperations().removeIterator(everythingClone, "fam9",
          EnumSet.of(IteratorUtil.IteratorScope.scan));
      assertFalse(client.tableOperations().listIterators(everythingClone).containsKey("fam9"));
      // TODO the iterator removal may not have made it to the tserver
      verifyData(client, everythingClone, new Authorizations("CAT", "DOG"),
          generateKeys(0, 100, tr -> tr.fam != 3));

      // test deleting a row ranges and scanning to verify its gone
      client.tableOperations().deleteRows(everythingClone, new Text(row(35)), new Text(row(40)));
      verifyData(client, everythingClone, new Authorizations("CAT", "DOG"),
          generateKeys(0, 100, tr -> (tr.row <= 35 || tr.row > 40) && tr.fam != 3));

      // the changes to the clone should not have affected the source table so verify it again
      verifyEverythingTable(client, everythingTable);

      // test renaming a table
      String tableIdBeforeRename = client.tableOperations().tableIdMap().get(everythingClone);
      client.tableOperations().rename(everythingClone, everythingClone + "9");
      assertFalse(client.tableOperations().list().contains(everythingClone));

      // renaming a table should not affect its data
      verifyData(client, everythingClone + "9", new Authorizations("CAT", "DOG"),
          generateKeys(0, 100, tr -> (tr.row <= 35 || tr.row > 40) && tr.fam != 3));
      String tableIdAfterRename = client.tableOperations().tableIdMap().get(everythingClone + "9");
      assertEquals(tableIdBeforeRename, tableIdAfterRename);

      // test deleting a table
      client.tableOperations().delete(everythingClone + "9");
      assertFalse(client.tableOperations().list().contains(everythingClone + "9"));

      // test exporting and importing a table as part of this test because the table has lots of
      // customizations.
      exportImport(client, everythingTable, everythingImport);

      verifyEverythingTable(client, everythingImport);
    }
  }

  private static final SortedSet<Text> everythingSplits =
      new TreeSet<>(List.of(new Text(row(33)), new Text(row(66))));
  private static final Map<String,Set<Text>> everythingLocalityGroups =
      Map.of("G1", Set.of(new Text(family(3)), new Text(family(7))));
  private static final SamplerConfiguration everythingSampleConfig =
      new SamplerConfiguration(RowThreeSampler.class.getName());

  private static NewTableConfiguration getEverythingTableConfig() {
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.withSplits(everythingSplits);
    ntc.setProperties(Map.of(Property.TABLE_DURABILITY.getKey(), "flush"));
    ntc.setTimeType(TimeType.LOGICAL);
    ntc.enableSummarization(SummarizerConfiguration.builder(FamilySummarizer.class).build());
    ntc.enableSampling(everythingSampleConfig);
    ntc.setLocalityGroups(everythingLocalityGroups);
    ntc.attachIterator(new IteratorSetting(100, "fam9", FamNineFilter.class),
        EnumSet.of(IteratorUtil.IteratorScope.scan));
    return ntc;
  }

  private static void verifyEverythingTable(AccumuloClient client, String table) throws Exception {
    assertEquals(TimeType.LOGICAL, client.tableOperations().getTimeType(table));
    assertEquals(everythingSampleConfig, client.tableOperations().getSamplerConfiguration(table));
    assertTrue(client.tableOperations().tableIdMap().keySet().contains(table));
    assertTrue(client.tableOperations().list().contains(table));
    assertEquals(everythingSplits, new TreeSet<>(client.tableOperations().listSplits(table)));
    assertTrue(client.tableOperations().exists(table));
    assertFalse(client.tableOperations().exists("61dc5ad6a4983abaf107e94321f3a37e37375267"));
    assertEquals(everythingLocalityGroups, client.tableOperations().getLocalityGroups(table));
    assertEquals(EnumSet.of(IteratorUtil.IteratorScope.scan),
        client.tableOperations().listIterators(table).get("fam9"));
    assertEquals(new IteratorSetting(100, "fam9", FamNineFilter.class), client.tableOperations()
        .getIteratorSetting(table, "fam9", IteratorUtil.IteratorScope.scan));

    verifyData(client, table, Authorizations.EMPTY,
        generateKeys(0, 100, tr -> tr.fam != 9 && tr.vis.isEmpty()));
    verifyData(client, table, new Authorizations("CAT"),
        generateKeys(0, 100, tr -> tr.fam != 9 && !tr.vis.equals("DOG&CAT")));
    verifyData(client, table, new Authorizations("CAT", "DOG"),
        generateKeys(0, 100, tr -> tr.fam != 9));

    // test setting range on scanner
    verifyData(client, table, Authorizations.EMPTY,
        generateKeys(6, 19, tr -> tr.fam == 8 && tr.vis.isEmpty()), scanner -> {
          if (scanner instanceof Scanner) {
            ((Scanner) scanner).setRange(new Range(row(6), row(18)));
          } else if (scanner instanceof BatchScanner) {
            ((BatchScanner) scanner).setRanges(List.of(new Range(row(6), row(18))));
          } else {
            throw new IllegalArgumentException();
          }

          scanner.fetchColumnFamily(new Text(family(8)));
        });

    try (var scanner = client.createBatchScanner(table, new Authorizations("CAT", "DOG"))) {
      // set multiple ranges on scanner
      scanner
          .setRanges(List.of(new Range(row(6), row(18)), new Range(row(27), true, row(37), false)));
      verifyData(scanner, generateKeys(6, 37, tr -> tr.fam != 9 && (tr.row <= 18 || tr.row >= 27)));
    }

    // test scanning sample data
    verifyData(client, table, Authorizations.EMPTY,
        generateKeys(3, 4, tr -> tr.fam != 9 && tr.vis.isEmpty()),
        scanner -> scanner.setSamplerConfiguration(everythingSampleConfig));
    verifyData(client, table, new Authorizations("CAT"),
        generateKeys(3, 4, tr -> tr.fam != 9 && !tr.vis.equals("DOG&CAT")),
        scanner -> scanner.setSamplerConfiguration(everythingSampleConfig));
    verifyData(client, table, new Authorizations("CAT", "DOG"),
        generateKeys(3, 4, tr -> tr.fam != 9),
        scanner -> scanner.setSamplerConfiguration(everythingSampleConfig));

    // test fetching column families
    verifyData(client, table, Authorizations.EMPTY,
        generateKeys(0, 100, tr -> tr.fam == 5 && tr.vis.isEmpty()),
        scanner -> scanner.fetchColumnFamily(new Text(family(5))));
    verifyData(client, table, new Authorizations("CAT"),
        generateKeys(0, 100, tr -> tr.fam == 5 && !tr.vis.equals("DOG&CAT")),
        scanner -> scanner.fetchColumnFamily(new Text(family(5))));
    verifyData(client, table, new Authorizations("CAT", "DOG"),
        generateKeys(0, 100, tr -> tr.fam == 5),
        scanner -> scanner.fetchColumnFamily(new Text(family(5))));

    // summary data does not exist until flushed
    client.tableOperations().flush(table, null, null, true);
    var summaries = client.tableOperations().summaries(table).retrieve();
    assertEquals(1, summaries.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(300, summaries.get(0).getStatistics().get("c:" + family(i)));
    }
    assertEquals(10,
        summaries.get(0).getStatistics().keySet().stream().filter(k -> k.startsWith("c:")).count());
  }

  private static void write(AccumuloClient client, String table, Collection<Mutation> mutations)
      throws Exception {

    try (var writer = client.createBatchWriter(table)) {
      for (var mutation : mutations) {
        writer.addMutation(mutation);
      }
    }

  }

  private static void verifyData(AccumuloClient client, String table, Authorizations auths,
      SortedMap<Key,Value> expectedData) throws Exception {
    try (var scanner = client.createScanner(table, auths)) {
      verifyData(scanner, expectedData);
    }

    try (var scanner = client.createBatchScanner(table, auths)) {
      scanner.setRanges(List.of(new Range()));
      verifyData(scanner, expectedData);
    }
  }

  private static void verifyData(AccumuloClient client, String table, Authorizations auths,
      SortedMap<Key,Value> expectedData, Consumer<ScannerBase> scannerConsumer) throws Exception {
    try (var scanner = client.createScanner(table, auths)) {
      scannerConsumer.accept(scanner);
      verifyData(scanner, expectedData);
    }

    try (var scanner = client.createBatchScanner(table, auths)) {
      scanner.setRanges(List.of(new Range()));
      scannerConsumer.accept(scanner);
      verifyData(scanner, expectedData);
    }
  }

  private static void verifyData(ScannerBase scanner, SortedMap<Key,Value> expectedData) {
    Map<Key,Value> seen = scanner.stream().map(e -> {
      Key nk = new Key(e.getKey());
      nk.setTimestamp(Long.MAX_VALUE);
      return new AbstractMap.SimpleEntry<>(nk, e.getValue());
    }).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    seen = new TreeMap<>(seen);

    assertEquals(expectedData, seen);
  }

  private static void bulkImport(AccumuloClient client, String table,
      List<SortedMap<Key,Value>> data) throws Exception {
    String tmp = getCluster().getTemporaryPath().toString();
    String dir = tmp + "/comp_bulk_" + UUID.randomUUID();

    getCluster().getFileSystem().mkdirs(new Path(dir));

    int count = 0;
    for (var keyValues : data) {
      try (var output = getCluster().getFileSystem().create(new Path(dir + "/f" + count + ".rf"));
          var writer = RFile.newWriter().to(output).build()) {
        writer.startDefaultLocalityGroup();
        for (Map.Entry<Key,Value> entry : keyValues.entrySet()) {
          writer.append(entry.getKey(), entry.getValue());
        }
      }
      count++;
    }

    client.tableOperations().importDirectory(dir).to(table).load();
  }

  static class TestRecord {
    final int row;
    final int fam;
    final int qual;
    final String vis;
    final int val;

    TestRecord(int row, int fam, int qual, String vis, int val) {
      this.row = row;
      this.fam = fam;
      this.qual = qual;
      this.vis = vis;
      this.val = val;
    }
  }

  static String row(int r) {
    return String.format("%06d", r);
  }

  static String family(int f) {
    return String.format("%04d", f);
  }

  static String qualifier(int q) {
    return String.format("%04d", q);
  }

  static String value(int v) {
    return String.format("%09d", v);
  }

  static SortedMap<Key,Value> generateKeys(int minRow, int maxRow,
      Predicate<TestRecord> predicate) {
    return generateKeys(minRow, maxRow, 0, predicate);
  }

  static SortedMap<Key,Value> generateKeys(int minRow, int maxRow, int salt,
      Predicate<TestRecord> predicate) {
    TreeMap<Key,Value> data = new TreeMap<>();
    var mutations = generateMutations(minRow, maxRow, salt, predicate);
    for (Mutation m : mutations) {
      for (var cu : m.getUpdates()) {
        Key k = new Key(m.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
            cu.getColumnVisibility());
        Value v = new Value(cu.getValue());
        data.put(k, v);
      }
    }

    return data;
  }

  static Collection<Mutation> generateMutations(int minRow, int maxRow,
      Predicate<TestRecord> predicate) {
    return generateMutations(minRow, maxRow, 0, predicate);
  }

  static Collection<Mutation> generateMutations(int minRow, int maxRow, int salt,
      Predicate<TestRecord> predicate) {

    List<Mutation> mutations = new ArrayList<>();

    for (int r = minRow; r < maxRow; r++) {
      Mutation m = new Mutation(row(r));
      for (int f = 0; f < 10; f++) {
        for (int q = 17; q < 20; q++) {
          String vis = "";
          int unsaltedVal = r << 16 | f << 8 | q;
          if (unsaltedVal % 5 == 0) {
            vis = "DOG&CAT";
          } else if (unsaltedVal % 11 == 0) {
            vis = "DOG|CAT";
          }

          int v = unsaltedVal ^ salt;

          if (predicate.test(new TestRecord(r, f, q, vis, v))) {
            m.put(family(f), qualifier(q), new ColumnVisibility(vis), value(v));
          }
        }
      }
      if (m.size() > 0) {
        mutations.add(m);
      }
    }

    return mutations;
  }

  private void exportImport(AccumuloClient client, String srcTable, String importTable)
      throws Exception {
    client.tableOperations().offline(srcTable, true);

    String tmp = getCluster().getTemporaryPath().toString();
    String exportDir = tmp + "/export_" + srcTable;
    String importDir = tmp + "/import_" + importTable;

    var fs = getCluster().getFileSystem();

    client.tableOperations().exportTable(srcTable, exportDir);

    fs.mkdirs(new Path(importDir));
    try (var reader =
        new BufferedReader(new InputStreamReader(fs.open(new Path(exportDir + "/distcp.txt"))))) {
      String line;
      while ((line = reader.readLine()) != null) {
        var srcPath = new Path(line);
        Path destPath = new Path(importDir + "/" + srcPath.getName());
        FileUtil.copy(fs, srcPath, fs, destPath, false, fs.getConf());
      }
    }

    client.tableOperations().importTable(importTable, importDir);

    client.tableOperations().online(srcTable, true);
  }

  public static class FamNineFilter extends Filter {

    @Override
    public boolean accept(Key k, Value v) {
      return !k.getColumnFamilyData().toString().equals(family(9));
    }
  }

  public static class FamThreeFilter extends Filter {

    @Override
    public boolean accept(Key k, Value v) {
      return !k.getColumnFamilyData().toString().equals(family(3));
    }
  }

  public static class RowThreeSampler implements Sampler {

    @Override
    public void init(SamplerConfiguration config) {

    }

    @Override
    public boolean accept(Key k) {
      return k.getRowData().toString().equals(row(3));
    }
  }

  public static class FamilySummarizer extends CountingSummarizer<String> {

    @Override
    protected Converter<String> converter() {
      return (k, v, consumer) -> consumer.accept(k.getColumnFamilyData().toString());
    }
  }

  public static class TestConstraint implements Constraint {

    @Override
    public String getViolationDescription(short violationCode) {
      if (violationCode == 1) {
        return "No numeric field seen";
      }

      return null;
    }

    private boolean isNumber(byte[] field) {
      try {
        Integer.parseInt(new String(field, UTF_8));
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
      if (!isNumber(mutation.getRow())) {
        return List.of((short) 1);
      }

      for (ColumnUpdate cu : mutation.getUpdates()) {
        if (!isNumber(cu.getColumnFamily()) || !isNumber(cu.getColumnQualifier())) {
          return List.of((short) 1);
        }
      }

      return null;
    }
  }
}
