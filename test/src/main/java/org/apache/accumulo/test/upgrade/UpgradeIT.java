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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
@Tag(MINI_CLUSTER_ONLY)
public class UpgradeIT extends WithTestNames {

  private static final Logger log = LoggerFactory.getLogger(UpgradeIT.class);

  @Test
  public void testCleanUpgrade() throws Exception {

    String testName = "cleanShutdown";
    var versions = UpgradeTestUtils.findVersions("cleanShutdown");

    for (var version : versions) {
      log.info("Running upgrade test: {} -> {}", version, Constants.VERSION);

      var originalDir = UpgradeTestUtils.getTestDir(version, testName);
      UpgradeTestUtils.backupOrRestore(version, testName);

      Assertions.assertNotEquals(version, Constants.VERSION);
      var newMacDir = UpgradeTestUtils.getTestDir(Constants.VERSION, testName);
      FileUtils.deleteQuietly(newMacDir);

      // TODO need more comments

      File csFile = new File(originalDir, "conf/hdfs-site.xml");
      Configuration hadoopSite = new Configuration();
      hadoopSite.set("fs.defaultFS", "file:///");
      try (OutputStream out =
          new BufferedOutputStream(new FileOutputStream(csFile.getAbsolutePath()))) {
        hadoopSite.writeXml(out);
      }

      MiniAccumuloConfigImpl config =
          new MiniAccumuloConfigImpl(newMacDir, UpgradeTestUtils.ROOT_PASSWORD);
      config.useExistingInstance(new File(originalDir, "conf/accumulo.properties"),
          new File(originalDir, "conf"));

      var cluster = new MiniAccumuloClusterImpl(config);

      cluster._exec(cluster.getConfig().getServerClass(ServerType.ZOOKEEPER), ServerType.ZOOKEEPER,
          Map.of(), new File(originalDir, "conf/zoo.cfg").getAbsolutePath());

      // TODO check root tablet metadata and ensure it has no location

      // TODO started processes continue to run
      cluster.start();

      try (var client = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
        try (var scanner = client.createScanner("test")) {
          scanner.forEach(System.out::println);
        }
      } finally {
        cluster.stop();
      }
    }
  }
}
