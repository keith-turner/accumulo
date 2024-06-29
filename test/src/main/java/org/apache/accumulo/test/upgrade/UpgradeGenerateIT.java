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
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.test.upgrade.UpgradeTestUtils.getOriginalMacDir;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessNotFoundException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Disabled
@Tag(MINI_CLUSTER_ONLY)
public class UpgradeGenerateIT {

  private MiniAccumuloClusterImpl cluster;

  private void setupUpgradeTest(String testName) throws Exception {

    File testDir = getOriginalMacDir(testName);

    FileUtils.deleteQuietly(testDir);

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
  public void cleanShutdown() throws Exception {

    setupUpgradeTest("cleanShutdown");
    try (AccumuloClient c = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
      c.tableOperations().create("test");
      try (var writer = c.createBatchWriter("test")) {
        var mutation = new Mutation("0");
        mutation.put("f", "q", "v");
        writer.addMutation(mutation);
      }

      cluster.getClusterControl().adminStopAll();
    } finally {
      cluster.stop();
    }
  }

  @Test
  public void dirtyShutdown() throws Exception {

    setupUpgradeTest("dirtyShutdown");
    try (AccumuloClient c = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
      c.tableOperations().create("test");
      try (var writer = c.createBatchWriter("test")) {
        var mutation = new Mutation("0");
        mutation.put("f", "q", "v");
        writer.addMutation(mutation);
      }

      // TODO run some eventual scans in 2.1 version of this test to leave some scan refs behind

      cluster.getProcesses().forEach((server, processes) -> {
        processes.forEach(process -> {
          try {
            cluster.killProcess(server, process);
          } catch (ProcessNotFoundException | InterruptedException e) {
            throw new IllegalStateException(e);
          }
        });
      });
    } finally {
      cluster.stop();
    }
  }
}
