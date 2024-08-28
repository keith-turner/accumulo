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
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ComputeLoadMapping {
  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    Map<String,String> tableProps = Map.of(); // TODO
    Path dirPath = new Path(args[1]);
    Executor executor = Executors.newFixedThreadPool(Integer.parseInt(args[2]));
    var client = Accumulo.newClient()
        .from(System.getenv("ACCUMULO_CONF_DIR") + "/accumulo-client.properties").build();
    var context = (ClientContext) client;
    int maxTablets = 1000;
    TableId tableId = context.getTableId(args[0]);
    BulkImport.computeFileToTabletMappings(fs, tableId, tableProps, dirPath, executor, context,
        maxTablets);
  }
}
