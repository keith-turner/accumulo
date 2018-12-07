/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.test;

import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.junit.Test;

public class IsolatedScannerIT  extends AccumuloClusterHarness {

  static final int ROWS = 1000;
  static final int COLS = 1000;

  @Test
  public void testManyScans() throws Exception {

    try (AccumuloClient client = getAccumuloClient()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, getClientInfo(), ROWS, COLS, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      //TODO test same case for regular scanner???
      for(int i = 0; i < 300; i++) {
        try(IsolatedScanner scanner = new IsolatedScanner(client.createScanner(tableName, Authorizations.EMPTY))) {
          scanner.setRange(new Range());
          int count = 0;
          for (Entry<Key,Value> entry : scanner) {
            if(count++ > 10) {
              break;
            }
          }
        }
      }

      List<String> tservers = client.instanceOperations().getTabletServers();
      for (String tserver : tservers) {
        System.out.println(tserver + " " + client.instanceOperations().getActiveScans(tserver).size());
      }
    }
  }
}
