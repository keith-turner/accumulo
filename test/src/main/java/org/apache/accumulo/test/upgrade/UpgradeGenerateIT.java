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
package org.apache.accumulo.test.upgrade;

import static org.apache.accumulo.core.conf.Property.GENERAL_PROCESS_BIND_ADDRESS;
import static org.apache.accumulo.core.conf.Property.TABLE_MAJC_RATIO;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.test.upgrade.UpgradeTestUtils.getTestDir;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ComprehensiveIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * <p>
 * This IT supports manual upgrade testing via the following process.
 *
 * <ol>
 * <li>Run this IT in version N of accumulo to generate data</li>
 * <li>Checkout version N+1 of accumulo. Do not run mvn clean as the persisted data is in
 * test/target and would be wiped</li>
 * <li>Run UpgradeIT which will upgrade and verify the persisted data created by this test.</li>
 * </ol>
 *
 *
 */

@Disabled
@Tag(MINI_CLUSTER_ONLY)
public class UpgradeGenerateIT {

  private MiniAccumuloClusterImpl cluster;

  private void setupUpgradeTest(String testName) throws Exception {

    UpgradeTestUtils.deleteTest(Constants.VERSION, testName);
    File testDir = getTestDir(Constants.VERSION, testName);

    MiniAccumuloConfigImpl config =
        new MiniAccumuloConfigImpl(testDir, UpgradeTestUtils.ROOT_PASSWORD);
    config.setProperty(GENERAL_PROCESS_BIND_ADDRESS, "localhost");
    cluster = new MiniAccumuloClusterImpl(config);
    Configuration haddopConfig = new Configuration(false);
    haddopConfig.set("fs.file.impl", RawLocalFileSystem.class.getName());
    File csFile = new File(Objects.requireNonNull(config.getConfDir()), "core-site.xml");
    try (OutputStream out =
        new BufferedOutputStream(new FileOutputStream(csFile.getAbsolutePath()))) {
      haddopConfig.writeXml(out);
    }

    cluster.start();
  }

  @Test
  public void baseline() throws Exception {

    setupUpgradeTest("baseline");
    try (AccumuloClient c = Accumulo.newClient().from(cluster.getClientProperties()).build()) {

      var table1 = "ut1";

      c.tableOperations().create(table1);
      try (var writer = c.createBatchWriter(table1)) {
        var mutations = ComprehensiveIT.generateMutations(0, 1000, 3, tr -> true);
        int written = 0;
        for (var mutation : mutations) {
          writer.addMutation(mutation);
          written++;
          if (written == 50) {
            // generate multiple files in the table
            writer.flush();
            c.tableOperations().flush(table1, null, null, true);
          }
        }
      }
      c.tableOperations().flush(table1, null, null, true);

      c.tableOperations().setProperty(table1, TABLE_MAJC_RATIO.getKey(), "3.14");

      // create an empty table
      var table2 = "ut2";
      c.tableOperations().create(table2);

      // create a table with splits
      var table3 = "ut3";
      c.tableOperations().create(table3);
      c.tableOperations().addSplits(table3, ComprehensiveIT.createSplits(0, 1000, 13));
      try (var writer = c.createBatchWriter(table3)) {
        var mutations = ComprehensiveIT.generateMutations(0, 1000, 7, tr -> true);
        for (var mutation : mutations) {
          writer.addMutation(mutation);
        }
      }

      c.tableOperations().setProperty(table3, TABLE_MAJC_RATIO.getKey(), "2.72");

      cluster.getClusterControl().adminStopAll();
    } finally {
      cluster.stop();
    }
  }
}
