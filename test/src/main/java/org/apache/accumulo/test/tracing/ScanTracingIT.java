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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class ScanTracingIT extends ConfigurableMacBase {

  private static List<String> getJvmArgs(){
    String javaAgent = null;
    for (var cpi : System.getProperty("java.class.path").split(":")) {
      if (cpi.contains("opentelemetry-javaagent")) {
        javaAgent = cpi;
      }
    }

    Objects.requireNonNull(javaAgent);

    return  List.of(
            "-Dotel.traces.exporter=otlp",
            "-Dotel.exporter.otlp.protocol=http/protobuf",
            "-Dotel.exporter.otlp.endpoint=http://localhost:12345", // TODO use default otlp port
            "-Dotel.metrics.exporter=none",
            "-Dotel.logs.exporter=none",
            "-javaagent:" + javaAgent
    );
  }

  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    getJvmArgs().forEach(cfg::addJvmOption);
  }

  @Test
  public void test() throws Exception {
    HttpServer server = HttpServer.create();
    server.bind(new InetSocketAddress("localhost", 12345), 100);
    HttpHandler handler = new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        System.out.println("headers : " + exchange.getRequestHeaders().keySet());
        System.out.println("method  : " + exchange.getRequestMethod());
        // TODO check for /v1/traces
        System.out.println("uri     : " + exchange.getRequestURI());

        var body = exchange.getRequestBody().readAllBytes();
        System.out.println("body len : " + body.length);
        try {
          var etsr = io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest.parseFrom(body);
          var spans = etsr.getResourceSpansList().stream()
                  .flatMap(r -> r.getScopeSpansList().stream())
                  .flatMap(r -> r.getSpansList().stream())
                  .collect(Collectors.toList());

          spans.forEach(s->{
            System.out.println("trace id: "+Hex.encodeHexString(s.getTraceId().toByteArray(), true)+" "+s.getName());
            s.getAttributesList().forEach(kv->{
              if(kv.getValue().hasIntValue()){
                System.out.println("  "+kv.getKey()+" "+kv.getValue().getIntValue());
              }else if(kv.getValue().hasStringValue()) {
                System.out.println("  "+kv.getKey()+" "+kv.getValue().getStringValue());
              }

            });
          });


        } catch (Throwable e) {
          e.printStackTrace();
          System.out.println("here2");
        }

        System.out.println("here");
        exchange.sendResponseHeaders(200, 0);
        exchange.getResponseBody().close();
      }
    };
    server.createContext("/", handler);
    server.start();
    System.out.println(server.getAddress());



    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create("test");
      try(var writer = client.createBatchWriter("test")){
        for(int i = 0; i < 1000; i++){
          Mutation m = new Mutation(String.format("%09x", i));
          m.put("f","q","v");
          writer.addMutation(m);
        }
      }
    }


    var proc = getCluster().exec(ScanTraceClient.class, getJvmArgs(), getCluster().getClientPropsPath(), "test");
    Assertions.assertEquals(0, proc.getProcess().waitFor());
    System.out.println("stdout:"+proc.readStdOut());

    Thread.sleep(60000);



  }
}
