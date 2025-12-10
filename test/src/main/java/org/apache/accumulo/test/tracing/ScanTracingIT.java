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
package org.apache.accumulo.test.tracing;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.gson.FormattingStyle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class ScanTracingIT extends ConfigurableMacBase {

  private static int OTLP_PORT = 12345;

  private static List<String> getJvmArgs() {
    String javaAgent = null;
    for (var cpi : System.getProperty("java.class.path").split(":")) {
      if (cpi.contains("opentelemetry-javaagent")) {
        javaAgent = cpi;
      }
    }

    Objects.requireNonNull(javaAgent);

    return List.of("-Dotel.traces.exporter=otlp", "-Dotel.exporter.otlp.protocol=http/protobuf",
        "-Dotel.exporter.otlp.endpoint=http://localhost:"+OTLP_PORT,
        "-Dotel.metrics.exporter=none", "-Dotel.logs.exporter=none", "-javaagent:" + javaAgent);
  }

  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    getJvmArgs().forEach(cfg::addJvmOption);
  }

  @Test
  public void test() throws Exception {

    TraceCollector collector = new TraceCollector("localhost", OTLP_PORT);

    var ingestParams = new TestIngest.IngestParams(getClientProperties(), "test");
    ingestParams.createTable=true;
    ingestParams.rows = 1000;
    ingestParams.cols = 10;

    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      TestIngest.ingest(client, ingestParams);
      client.tableOperations().flush("test", null, null, true);
    }

    var results = run(ScanTraceClient.class, "test");
    System.out.println(results);

    int count = 0;
    while (count < 2) {
      var span = collector.take();
      if ((span.name.contains("scan-batch") || span.name.contains("multiscan-batch"))
          && "1<<".equals(span.stringAttributes.get("accumulo.extent"))) {
        System.out.println(span);
        count++;
      } else {
        System.out.println("ignoring "+span);
      }
    }

    // TODO test scan across multiple tablet servers
    // TODO test scan with a range
    // TODO test batch scan the spins up multiple threads on a single tserver w/ the same trace id
    // TODO test isolated scans
    // TODO test scan w/ large batch size
    // TODO test scan w/ table data cache enabled
    // TODO test concurrent scans of the same table w/ diff trace ids
    // TODO test scan of multiple tables (check table id, etc)

    Thread.sleep(60000);
  }

  public static void printResult(Map<String,String> result) {
    var gson = new GsonBuilder().setFormattingStyle(FormattingStyle.COMPACT).create();
    System.out.println("RESULT:" + gson.toJson(result));
  }

  public Map<String,String> run(Class<?> clazz, String... args)
      throws IOException, InterruptedException {
    var allArgs = Stream.concat(Stream.of(getCluster().getClientPropsPath()), Stream.of(args))
        .toArray(String[]::new);
    var proc = getCluster().exec(ScanTraceClient.class, getJvmArgs(), allArgs);
    Assertions.assertEquals(0, proc.getProcess().waitFor());
    var out = proc.readStdOut();
    var result = Arrays.stream(out.split("\\n")).filter(line -> line.startsWith("RESULT:"))
        .findFirst().orElse("RESULT:{}");
    result = result.substring("RESULT:".length());
    Type typeOfHashMap = new TypeToken<Map<String,String>>() {}.getType();
    Map<String,String> newMap = new Gson().fromJson(result, typeOfHashMap);
    return newMap;
  }
}
