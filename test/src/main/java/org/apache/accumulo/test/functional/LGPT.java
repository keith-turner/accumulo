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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;

public class LGPT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setMemory(ServerType.TABLET_SERVER, 2, MemoryUnit.GIGABYTE);
    cfg.setProperty(Property.TSERV_DATACACHE_SIZE.getKey(), "500M");
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED.getKey(), "true");
  }

  @Test
  public void testLgp() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      var ntc = new NewTableConfiguration().setLocalityGroups(Map.of("g1", Set.of(new Text("fam1"), new Text("fam2"), new Text("fam3")), "g2",Set.of(new Text("fam4"), new Text("fam5"), new Text("fam6"))));
      var props = Map.of(Property.TABLE_MAJC_RATIO.getKey(), "1", Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
      ntc.setProperties(props);
      client.tableOperations().create("lgpt", ntc);

      try (var writer = client.createBatchWriter("lgpt")) {
        for (int i = 0; i < 1_000_000; i++) {
          Mutation m = new Mutation(String.format("%06x", i));
          for (int j = 0; j < 10; j++) {
            m.put("fam" + j, "q", i * 10 + j + "");
          }
          writer.addMutation(m);
        }
      }

      var executor = Executors.newCachedThreadPool();

      for(int i = 0; i< 8; i++) {
        executor.submit(() -> {
          Random rand = new Random();
          try (var writer = client.createBatchWriter("lgpt")) {
            while (true) {
              int row = rand.nextInt(1000000);
              int fam = rand.nextInt(10);
              int value = rand.nextInt(10000000);
              Mutation m = new Mutation(String.format("%06x", row));
              m.put("fam" + fam, "q", value + "");
              writer.addMutation(m);
              if (rand.nextInt(100) == 0) {
                writer.flush();
              }
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
      }




      List<List<String>> cols = new ArrayList<>();
      cols.add(List.of("fam1","fam4", "fam7"));
      //cols.add(List.of("fam1","fam2", "fam3"));
      //cols.add(List.of("fam7","fam8", "fam9"));

      for(int i = 0; i< 100; i++) {
        for(var c : cols) {
          try(var scanner = client.createScanner("lgpt")) {
            c.forEach(scanner::fetchColumnFamily);
            int count = 0;
            long t1 = System.nanoTime();
            for(var e : scanner){
              count++;
            }
            long t2 = System.nanoTime();
            System.out.println(c+" "+((t2-t1)/1000000.0));
          }
        }
      }



    }
  }
}
