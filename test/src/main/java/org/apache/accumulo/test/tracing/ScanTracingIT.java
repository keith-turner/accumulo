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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.FormattingStyle;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScanTracingIT extends ConfigurableMacBase {

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
    // sized such that full table scans will not fit in the cache
    cfg.setProperty(Property.TSERV_DATACACHE_SIZE.getKey(), "8M");
  }

  private TraceCollector collector;

  @BeforeEach
  public void startCollector() throws IOException {
    collector = new TraceCollector("localhost", OTLP_PORT);
  }

  @AfterEach
  public void stopCollector() throws IOException {
    collector.stop();
  }

  @Test
  public void test() throws Exception{
    var names = getUniqueNames(7);
    runTest(names[0], 0, false, false, -1, -1, -1);
    runTest(names[1], 10, false, false, -1, -1, -1);
    runTest(names[2], 0, true, false, -1, -1, -1);
    runTest(names[3], 0, false, false, -1, -1, 2);
    runTest(names[4], 0, false, false, 32, 256, -1);
    runTest(names[5], 0, true, true,32, 256, -1);
    runTest(names[6], 0, true, false, -1, -1, 2);
  }

  private void runTest(String tableName, int numSplits, boolean cacheData, boolean secondScanFitsInCache, int startRow, int endRow, int column) throws Exception {

    var ingestParams = new TestIngest.IngestParams(getClientProperties(), tableName);
    ingestParams.createTable=false;
    ingestParams.rows = 1000;
    ingestParams.cols = 10;

    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      var ntc = new NewTableConfiguration();
      if(numSplits > 0){
        var splits = TestIngest.getSplitPoints(0, 1000, numSplits);
        ntc.withSplits(splits);
      }

      if(cacheData){
        ntc.setProperties(Map.of(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true"));
      }

      client.tableOperations().create(tableName, ntc);

      TestIngest.ingest(client, ingestParams);
      client.tableOperations().flush(tableName, null, null, true);
    }

    long expectedRows = ingestParams.rows;

    var options = new ScanTraceClient.Options(tableName);
    if(startRow != -1 && endRow != -1) {
      options.startRow = TestIngest.generateRow(startRow, 0).toString();
      options.endRow = TestIngest.generateRow(endRow, 0).toString();
      expectedRows = IntStream.range(startRow, endRow).count();
    }

    int expectedColumns = ingestParams.cols;

    if(column != -1) {
      var col = TestIngest.generateColumn(ingestParams, column);
      options.family = col.getColumnFamily().toString();
      options.qualifier = col.getColumnQualifier().toString();
      expectedColumns = 1;
    }

    var results = run(ScanTraceClient.class, options);
    System.out.println(results);

    var tableId = getServerContext().getTableId(tableName).canonical();

    Map<String, Long> scanStats = new TreeMap<>();
    Map<String, Long> batchScanStats = new TreeMap<>();
    Set<String> extents1 = new TreeSet<>();
    Set<String> extents2 = new TreeSet<>();

    while (scanStats.getOrDefault("accumulo.entries.returned",0L) < expectedRows * expectedColumns || batchScanStats.getOrDefault("accumulo.entries.returned",0L) < expectedRows * expectedColumns) {
      var span = collector.take();
      if (span.name.contains("scan-batch") && span.stringAttributes.get("accumulo.table.id").equals(tableId) && (results.get("traceId1").equals(span.traceId) || results.get("traceId2").equals(span.traceId))){
        assertEquals("default", span.stringAttributes.get("accumulo.executor"));
        if(numSplits == 0) {
          assertEquals(tableId + "<<", span.stringAttributes.get("accumulo.extent"));
        }else{
          var extent  = span.stringAttributes.get("accumulo.extent");
          assertTrue(extent.startsWith(tableId+";") || extent.startsWith(tableId+"<"));
        }
        assertEquals(1, span.integerAttributes.get("accumulo.seeks"));
        if(span.name.contains("multiscan-batch")){
          assertEquals(results.get("traceId1"), span.traceId);
          extents1.add(span.stringAttributes.get("accumulo.extent"));
        }else{
          assertEquals(results.get("traceId2"), span.traceId);
          extents2.add(span.stringAttributes.get("accumulo.extent"));
        }
      }else{
        continue;
      }

      if (span.name.contains("multiscan-batch")){
        span.integerAttributes.forEach((k,v)->{
          batchScanStats.merge(k, v, Long::sum);
        });
      } else {
        span.integerAttributes.forEach((k,v)->{
          scanStats.merge(k, v, Long::sum);
        });
      }
    }

    if(numSplits > 0) {
      assertEquals(numSplits, extents1.size());
      assertEquals(numSplits, extents2.size());
    }

    // TODO count the blocks in the rfile to know what cache counts should be

    System.out.println("scanStats "+scanStats);
    System.out.println("batchScanStats "+batchScanStats);

    assertEquals( expectedRows * expectedColumns,Long.parseLong(results.get("scanCount")), results::toString);

    var statsList = List.of(batchScanStats, scanStats);
    for(int i = 0; i<statsList.size(); i++){
      var statsMap = statsList.get(i);
      assertEquals(expectedRows * 10, statsMap.get("accumulo.entries.read"), statsMap::toString);
      assertEquals(Long.parseLong(results.get("scanCount")), statsMap.get("accumulo.entries.returned"), statsMap::toString);
      // When filtering on columns will read more data than we return
      double colMultiplier = 10.0/expectedColumns;
      assertClose((long)(Long.parseLong(results.get("scanSize")) * colMultiplier), statsMap.get("accumulo.bytes.read"), .05);
      assertClose(Long.parseLong(results.get("scanSize")), statsMap.get("accumulo.bytes.returned"), .05);
      if(secondScanFitsInCache && i == 1){
        assertEquals(0,  statsMap.get("accumulo.bytes.read.file"), statsMap::toString);
      }else {
        assertClose((long) (statsMap.get("accumulo.bytes.read") * .005), statsMap.get("accumulo.bytes.read.file"), .2);
      }
      if(cacheData) {
        assertEquals(0, statsMap.get("accumulo.cache.data.bypasses"), statsMap::toString);
        assertTrue(statsMap.get("accumulo.cache.data.hits") + statsMap.get("accumulo.cache.data.misses") > 0, statsMap::toString);
        if(statsMap.get("accumulo.bytes.read.file") == 0){
            assertEquals(0L,  statsMap.get("accumulo.cache.data.misses"), statsMap::toString);
        }
        // When caching data, does not seem to hit the cache much
        var cacheSum = statsMap.get("accumulo.cache.index.hits") + statsMap.get("accumulo.cache.index.misses");
        assertTrue(cacheSum == 0 || cacheSum == 1, statsMap::toString);
      } else {
        assertEquals(0, statsMap.get("accumulo.cache.data.hits"), statsMap::toString);
        assertEquals(0, statsMap.get("accumulo.cache.data.misses"), statsMap::toString);
        assertTrue(statsMap.get("accumulo.cache.data.bypasses") > statsMap.get("accumulo.seeks"), statsMap::toString);
        // When not caching data, will go to the index cache each time a block location is looked up.  TODO why is this happening? keeps getting the RootData metablock for every data block.
        assertClose(statsMap.get("accumulo.cache.data.bypasses"),  statsMap.get("accumulo.cache.index.hits"), .05);
      }
      assertEquals(0,  statsMap.get("accumulo.cache.index.bypasses"), statsMap::toString);
    }

    // TODO test scan across multiple tablet servers
    // TODO test scan with a range
    // TODO test batch scan the spins up multiple threads on a single tserver w/ the same trace id
    // TODO test isolated scans
    // TODO test scan w/ large batch size
    // TODO test scan w/ table data cache enabled
    // TODO test concurrent scans of the same table w/ diff trace ids
    // TODO test scan of multiple tables (check table id, etc)
    // TODO test w/ filtering
    // TODO test multi-level rfile

  }

  public void assertClose(long expected, long value, double e){
    assertTrue(Math.abs(1-(double)expected/(double)value) < e, ()->expected+" "+value+" "+e);
  };

  public static void printResult(Map<String,String> result) {
    var gson = new GsonBuilder().setFormattingStyle(FormattingStyle.COMPACT).create();
    System.out.println("RESULT:" + gson.toJson(result));
  }

  public Map<String,String> run(Class<?> clazz, ScanTraceClient.Options opts)
      throws IOException, InterruptedException {
    opts.clientPropsPath = getCluster().getClientPropsPath();
    new Gson().toJson(opts);
    var proc = getCluster().exec(ScanTraceClient.class, getJvmArgs(), new Gson().toJson(opts));
    assertEquals(0, proc.getProcess().waitFor());
    var out = proc.readStdOut();
    var result = Arrays.stream(out.split("\\n")).filter(line -> line.startsWith("RESULT:"))
        .findFirst().orElse("RESULT:{}");
    result = result.substring("RESULT:".length());
    Type typeOfHashMap = new TypeToken<Map<String,String>>() {}.getType();
    Map<String,String> newMap = new Gson().fromJson(result, typeOfHashMap);
    return newMap;
  }
}
