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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TraceCollector {
  // TODO use jetty
  private final HttpServer server;

  private final LinkedBlockingQueue<SpanData> spanQueue = new LinkedBlockingQueue<>();

  private class TraceHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      var body = exchange.getRequestBody().readAllBytes();
      try {
        var etsr =
            io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest.parseFrom(body);
        var spans =
            etsr.getResourceSpansList().stream().flatMap(r -> r.getScopeSpansList().stream())
                .flatMap(r -> r.getSpansList().stream()).collect(Collectors.toList());

        spans.forEach(s -> {
          var traceId = Hex.encodeHexString(s.getTraceId().toByteArray(), true);

          Map<String,String> stringAttrs = new HashMap<>();
          Map<String,Long> intAttrs = new HashMap<>();

          s.getAttributesList().forEach(kv -> {
            if (kv.getValue().hasIntValue()) {
              intAttrs.put(kv.getKey(), kv.getValue().getIntValue());
            } else if (kv.getValue().hasStringValue()) {
              stringAttrs.put(kv.getKey(), kv.getValue().getStringValue());
            }
          });

          spanQueue.add(
              new SpanData(traceId, s.getName(), Map.copyOf(stringAttrs), Map.copyOf(intAttrs)));
        });

      } catch (Throwable e) {
        // TODO need to fail test
        e.printStackTrace();
      }
      exchange.sendResponseHeaders(200, 0);
      exchange.getResponseBody().close();
    }
  };

  TraceCollector(String host, int port) throws IOException {
    server = HttpServer.create();
    server.bind(new InetSocketAddress("localhost", 12345), 100);
    server.createContext("/v1/traces", new TraceHandler());
    server.start();
  }

  SpanData take() throws InterruptedException {
    return spanQueue.take();
  }

  void stop() {
    server.stop(0);
  }
}
