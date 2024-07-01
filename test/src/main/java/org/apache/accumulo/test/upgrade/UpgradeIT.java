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
import static org.apache.accumulo.test.ComprehensiveBaseIT.createSplits;
import static org.apache.accumulo.test.ComprehensiveBaseIT.generateKeys;
import static org.apache.accumulo.test.ComprehensiveBaseIT.scan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ComprehensiveIT;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
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
  public void testBaseline() throws Exception {
    runUpgradeTest("baseline", client -> {
      var seenTables = client.tableOperations().list().stream()
          .filter(tableName -> !tableName.startsWith("accumulo.")).collect(Collectors.toSet());
      assertEquals(Set.of("ut1", "ut2", "ut3"), seenTables);

      assertEquals(generateKeys(0, 1000, 3, tr -> true),
          scan(client, "ut1", ComprehensiveIT.AUTHORIZATIONS));
      assertEquals(Map.of(), scan(client, "ut2", Authorizations.EMPTY));
      // TODO change salt
      assertEquals(generateKeys(0, 1000, 3, tr -> true),
          scan(client, "ut3", ComprehensiveIT.AUTHORIZATIONS));

      assertEquals(Set.of(), client.tableOperations().listSplits("ut1"));
      assertEquals(Set.of(), client.tableOperations().listSplits("ut2"));
      assertEquals(createSplits(0, 1000, 13), client.tableOperations().listSplits("ut3"));

      assertEquals("3.14", client.tableOperations().getTableProperties("uti1")
          .get(Property.TABLE_MAJC_RATIO.getKey()));
      assertNull(client.tableOperations().getTableProperties("uti2")
          .get(Property.TABLE_MAJC_RATIO.getKey()));
      assertEquals("2.72", client.tableOperations().getTableProperties("uti3")
          .get(Property.TABLE_MAJC_RATIO.getKey()));

    });
  }

  private interface Verifier {
    void verify(AccumuloClient client) throws Exception;
  }

  private void runUpgradeTest(String testName, Verifier verifier) throws Exception {
    var versions = UpgradeTestUtils.findVersions("baseline");

    for (var version : versions) {
      if (version.equals(Constants.VERSION)) {
        log.info("Skipping self {} ", Constants.VERSION);
        continue;
      }

      log.info("Running upgrade test: {} -> {}", version, Constants.VERSION);

      var originalDir = UpgradeTestUtils.getTestDir(version, testName);
      UpgradeTestUtils.backupOrRestore(version, testName);

      var newMacDir = UpgradeTestUtils.getTestDir(Constants.VERSION, testName);
      FileUtils.deleteQuietly(newMacDir);

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

      cluster.start();

      try (var client = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
        verifier.verify(client);
      } finally {
        // The cluster stop method will not kill processes because the cluster was started using an
        // existing instance.
        UpgradeTestUtils.killAll(cluster);
        cluster.stop();
      }
    }
  }
}
