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
package org.apache.accumulo.core.clientImpl.bulk;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ComputeLoadMapping {
  public static void main(String[] args) throws Exception {

    if(args.length != 3) {
      System.err.println("Usage : "+ComputeLoadMapping.class.getSimpleName()+" <table> <dir> <num threads>");
      System.exit(-1);
    }

    String tableName = args[0];
    String dir = args[1];
    int numThreads = Integer.parseInt(args[2]);

    String propsFile = System.getenv("ACCUMULO_CONF_DIR") + "/accumulo-client.properties";

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    try(var client =  Accumulo.newClient().from(propsFile).build()) {
      FileSystem fs = FileSystem.get(new Configuration());
      Path dirPath = new Path(dir);
      var context = (ClientContext) client;
      int maxTablets = 1000;
      TableId tableId = context.getTableId(tableName);
      Map<String, String> tableProps = client.tableOperations().getConfiguration(tableName);
      BulkImport.computeFileToTabletMappings(fs, tableId, tableProps, dirPath, executor, context,
              maxTablets);
    }finally {
      executor.shutdownNow();
    }
  }
}
