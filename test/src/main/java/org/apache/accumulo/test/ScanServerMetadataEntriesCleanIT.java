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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.metadata.ScanServerRefTabletFile;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.ScanServerMetadataEntries;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerMetadataEntriesCleanIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void start() throws Exception {
    startMiniCluster();
  }

  @AfterAll
  public static void stop() throws Exception {
    stopMiniCluster();
  }

  @Test
  public void testServerContextMethods() throws Exception {

    try (AccumuloClient ac = Accumulo.newClient().from(getClientProps()).build()) {
      HostAndPort server = HostAndPort.fromParts("127.0.0.1", 1234);
      UUID serverLockUUID = UUID.randomUUID();

      String[] files =
          new String[] {"hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf",
              "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf"};

      Set<ScanServerRefTabletFile> scanRefs = new HashSet<>();
      for (String file : files) {
        scanRefs.add(new ScanServerRefTabletFile(file, server.toString(), serverLockUUID));
      }

      ServerContext ctx = getCluster().getServerContext();

      ctx.getAmple().putScanServerFileReferences(scanRefs);
      assertEquals(2, ctx.getAmple().getScanServerFileReferences().count());

      Set<ScanServerRefTabletFile> scanRefs2 =
          ctx.getAmple().getScanServerFileReferences().collect(Collectors.toSet());
      assertEquals(scanRefs, scanRefs2);

      ScanServerMetadataEntries.clean(ctx);
      assertEquals(0, ctx.getAmple().getScanServerFileReferences().count());

    }
  }
}