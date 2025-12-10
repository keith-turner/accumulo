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

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.data.Range;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

public class ScanTraceClient {
  public static void main(String[] args) throws Exception {
    String clientPropsPath = args[0];
    String table = args[1];

    Tracer tracer = GlobalOpenTelemetry.get().getTracer(ScanTraceClient.class.getName());
    Span span = tracer.spanBuilder("test-scan").startSpan();

    long scanCount = 0;
    long batchScancount = 0;

    try (var client = Accumulo.newClient().from(clientPropsPath).build();
        var scope = span.makeCurrent()) {
      try (var scanner = client.createBatchScanner(table)) {
        scanner.setRanges(List.of(new Range()));
        batchScancount = scanner.stream().count();
      }
      try (var scanner = client.createScanner(table)) {
        scanner.setBatchSize(10_000);
        scanCount = scanner.stream().count();
      }
    } finally {
      span.end();
    }

    ScanTracingIT
        .printResult(Map.of("traceId",span.getSpanContext().getTraceId(), "scanCount", scanCount + "", "batchScanCount", batchScancount + ""));
  }
}
