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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class FileMetadataIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(6);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  // private static final Logger log = LoggerFactory.getLogger(FileMetadataIT.class);
  static final int COLS = 1;
  static final String COLF = "colf";

  public static void ingest(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String tableName) throws Exception {
    IngestParams params = new IngestParams(accumuloClient.properties(), tableName, rows);
    params.cols = cols;
    params.dataSize = width;
    params.startRow = offset;
    params.columnFamily = COLF;
    params.createTable = true;
    TestIngest.ingest(accumuloClient, params);
  }

  private static void verify(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String tableName) throws Exception {
    VerifyParams params = new VerifyParams(accumuloClient.properties(), tableName, rows);
    params.rows = rows;
    params.dataSize = width;
    params.startRow = offset;
    params.columnFamily = COLF;
    params.cols = cols;
    VerifyIngest.verifyIngest(accumuloClient, params);
  }

  public static Text t(String s) {
    return new Text(s);
  }

  public static Mutation m(String row, String cf, String cq, String value) {
    Mutation m = new Mutation(t(row));
    m.put(t(cf), t(cq), new Value(value));
    return m;
  }

  @Test
  public void contiguousRangeTest() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      // Need permission to write to metadata
      accumuloClient.securityOperations().grantTablePermission(accumuloClient.whoami(),
          MetadataTable.NAME, TablePermission.WRITE);

      final int rows = 10000;
      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      final TableId tableId =
          TableId.of(accumuloClient.tableOperations().tableIdMap().get(tableName));

      // Ingest 10000 rows start with row 1 and flush and verify data
      ingest(accumuloClient, rows, COLS, 10, 1, tableName);
      accumuloClient.tableOperations().flush(tableName, null, null, true);
      verify(accumuloClient, rows, COLS, 10, 1, tableName);

      // Bring tablet offline so we can modify file metadata
      accumuloClient.tableOperations().offline(tableName, true);

      try (Scanner mdScanner =
          accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
        mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

        // Read each file, should just be 1.
        // Split into multiple ranges that are contiguous and store
        for (Entry<Key,Value> entry : mdScanner) {
          StoredTabletFile file =
              new StoredTabletFile(entry.getKey().getColumnQualifier().toString());
          DataFileValue value = new DataFileValue(entry.getValue().toString());
          final KeyExtent ke = KeyExtent.fromMetaRow(entry.getKey().getRow());

          // Create a mutation to delete the existing file metadata entry with infinite range
          TabletMutator mutator = ctx.getAmple().mutateTablet(ke);
          mutator.deleteFile(file);

          // Add 4 contiguous ranges
          final Mutation add = new Mutation(ke.toMetaRow());
          final int ranges = 4;
          for (int i = 1; i <= ranges; i++) {
            int rowsPerRange = rows / ranges;
            int endRow = i * rowsPerRange;

            mutator.putFile(
                StoredTabletFile.of(file.getPath(),
                    new Range(new Text("row_" + String.format("%010d", endRow - rowsPerRange)),
                        false, new Text("row_" + String.format("%010d", endRow)), true)),
                new DataFileValue(value.getSize() / ranges, value.getNumEntries() / ranges));
          }
          mutator.mutate();
        }
      }

      accumuloClient.tableOperations().online(tableName, true);
      verify(accumuloClient, rows, COLS, 10, 1, tableName);
    }
  }

  @Test
  public void fencedRangeTest() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      // Need permission to write to metadata
      accumuloClient.securityOperations().grantTablePermission(accumuloClient.whoami(),
          MetadataTable.NAME, TablePermission.WRITE);

      final int rows = 10000;
      final int ranges = 4;
      int rowsPerRange = rows / ranges;

      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      final TableId tableId =
          TableId.of(accumuloClient.tableOperations().tableIdMap().get(tableName));

      // Ingest 10000 rows start with row 1 and flush and verify data
      ingest(accumuloClient, rows, COLS, 10, 1, tableName);
      accumuloClient.tableOperations().flush(tableName, null, null, true);
      verify(accumuloClient, rows, COLS, 10, 1, tableName);

      // Bring tablet offline so we can modify file metadata
      accumuloClient.tableOperations().offline(tableName, true);

      try (Scanner mdScanner =
          accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
        mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

        // Read each file, should just be 1.
        // Split into 4 ranges and skip the second so only 3/4 of the file should be readable
        for (Entry<Key,Value> entry : mdScanner) {
          StoredTabletFile file =
              new StoredTabletFile(entry.getKey().getColumnQualifier().toString());
          DataFileValue value = new DataFileValue(entry.getValue().toString());
          final KeyExtent ke = KeyExtent.fromMetaRow(entry.getKey().getRow());

          // Create a mutation to delete the existing file metadata entry with infinite range
          TabletMutator mutator = ctx.getAmple().mutateTablet(ke);
          mutator.deleteFile(file);

          // Add 3 ranges
          final Mutation add = new Mutation(ke.toMetaRow());

          for (int i = 1; i <= ranges; i++) {
            // Skip second range
            if (i == 2) {
              continue;
            }
            int endRow = i * rowsPerRange;
            mutator.putFile(
                StoredTabletFile.of(file.getPath(),
                    new Range(new Text("row_" + String.format("%010d", endRow - rowsPerRange)),
                        false, new Text("row_" + String.format("%010d", endRow)), true)),
                new DataFileValue(value.getSize() / ranges, value.getNumEntries() / ranges));
          }

          mutator.mutate();
        }
      }

      // Write mutations to metadata and then bring the table back online
      accumuloClient.tableOperations().online(tableName, true);

      // Verify rows 1 - 2500 are readable
      verify(accumuloClient, rowsPerRange, COLS, 10, 1, tableName);
      // Rows 2501 - 5000 should not be fenced and not visible
      // Try and read 2500 rows and verify none are visible, should throw an exception with 0 rows
      // read
      verifyNoRows(accumuloClient, rowsPerRange, COLS, 10, rowsPerRange + 1, tableName);
      // Verify rows 5001 - 10000 are readable
      verify(accumuloClient, rowsPerRange * 2, COLS, 10, (rowsPerRange * 2) + 1, tableName);

      // accumuloClient.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      //
      // // Verify rows 1 - 2500 are readable
      // verify(accumuloClient, rowsPerRange, COLS, 10, 1, tableName);
      // // Rows 2501 - 5000 should not be fenced and not visible
      // // Try and read 2500 rows and verify none are visible, should throw an exception with 0
      // rows
      // // read
      // verifyNoRows(accumuloClient, rowsPerRange, COLS, 10, rowsPerRange + 1, tableName);
      // // Verify rows 5001 - 10000 are readable
      // verify(accumuloClient, rowsPerRange * 2, COLS, 10, (rowsPerRange * 2) + 1, tableName);
    }
  }

  @Test
  public void splitsRangeTest() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      // Need permission to write to metadata
      accumuloClient.securityOperations().grantTablePermission(accumuloClient.whoami(),
          MetadataTable.NAME, TablePermission.WRITE);

      final int rows = 100000;
      final String tableName = getUniqueNames(1)[0];
      accumuloClient.tableOperations().create(tableName);
      final TableId tableId =
          TableId.of(accumuloClient.tableOperations().tableIdMap().get(tableName));

      // Divide table into 10 tablets with end rows of 10000, 20000, etc.
      final SortedSet<Text> splits = new TreeSet<>();
      for (int i = 1; i <= 10; i++) {
        splits.add(new Text("row_" + String.format("%010d", ((i * 10000)))));
      }

      // Ingest 100000 rows start with row 1 and flush and verify data
      accumuloClient.tableOperations().addSplits(tableName, splits);
      ingest(accumuloClient, rows, COLS, 10, 1, tableName);
      accumuloClient.tableOperations().flush(tableName, null, null, true);
      verify(accumuloClient, rows, COLS, 10, 1, tableName);

      // Bring tablet offline so we can modify file metadata
      accumuloClient.tableOperations().offline(tableName, true);

      StoredTabletFile file;

      try (Scanner mdScanner =
          accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
        mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

        // Read each of the 10 files referenced by the table, should be 1 per tablet
        for (Entry<Key,Value> entry : mdScanner) {
          file = new StoredTabletFile(entry.getKey().getColumnQualifier().toString());

          final KeyExtent ke = KeyExtent.fromMetaRow(entry.getKey().getRow());
          final int endRow = Integer.parseInt(ke.endRow().toString().replace("row_", ""));

          DataFileValue value = new DataFileValue(entry.getValue().toString());

          // Create a mutation to delete the existing file metadata entry with infinite range
          TabletMutator mutator = ctx.getAmple().mutateTablet(ke);
          mutator.deleteFile(file);

          mutator.putFile(
              StoredTabletFile.of(file.getPath(),
                  new Range(new Text("row_" + String.format("%010d", (endRow - 10000))), false,
                      new Text("row_" + String.format("%010d", (endRow - 5000))), true)),
              new DataFileValue(value.getSize() / 2, value.getNumEntries() / 2));
          mutator.putFile(
              StoredTabletFile.of(file.getPath(),
                  new Range(new Text("row_" + String.format("%010d", (endRow - 5000))), false,
                      new Text("row_" + String.format("%010d", endRow)), true)),
              new DataFileValue(value.getSize() / 2, value.getNumEntries() / 2));

          mutator.mutate();
        }

      }

      accumuloClient.tableOperations().online(tableName, true);
      verify(accumuloClient, rows, COLS, 10, 1, tableName);

      // try (Scanner mdScanner =
      // accumuloClient.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      // mdScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      // mdScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
      //
      // // Read each file referenced by that table
      // for (Entry<Key,Value> entry : mdScanner) {
      // file = new StoredTabletFile(entry.getKey().getColumnQualifier().toString());
      // System.out.println(file.getMetadata());
      // }
      // }
    }
  }

  // In the future we should probably enhance the ingest verify code to be able to better verify
  // ranges
  // but for now we can at least verify no rows are read by checking the exception
  private static void verifyNoRows(AccumuloClient accumuloClient, int rows, int cols, int width,
      int offset, String tableName) throws Exception {
    try {
      verify(accumuloClient, rows, cols, width, offset, tableName);
      fail("Should have failed");
    } catch (AccumuloException e) {
      assertTrue(e.getMessage().contains("Did not read expected number of rows. Saw 0"));
    }

  }

}
