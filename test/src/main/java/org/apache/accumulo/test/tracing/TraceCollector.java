package org.apache.accumulo.test.tracing;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class TraceCollector {
    private final HttpServer server;

    private final LinkedBlockingQueue<SpanData> spanQueue = new LinkedBlockingQueue<>();

    private class TraceHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var body = exchange.getRequestBody().readAllBytes();
            try {
                var etsr = io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest.parseFrom(body);
                var spans = etsr.getResourceSpansList().stream()
                        .flatMap(r -> r.getScopeSpansList().stream())
                        .flatMap(r -> r.getSpansList().stream())
                        .collect(Collectors.toList());

                spans.forEach(s->{
                    var traceId = Hex.encodeHexString(s.getTraceId().toByteArray(), true);

                    Map<String,String> stringAttrs = new HashMap<>();
                    Map<String, Long> intAttrs = new HashMap<>();

                    s.getAttributesList().forEach(kv->{
                        if(kv.getValue().hasIntValue()){
                            intAttrs.put(kv.getKey(), kv.getValue().getIntValue());
                        }else if(kv.getValue().hasStringValue()) {
                            stringAttrs.put(kv.getKey(), kv.getValue().getStringValue());
                        }
                    });

                    spanQueue.add(new SpanData(traceId, s.getName(), Map.copyOf(stringAttrs), Map.copyOf(intAttrs)));
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

    void stop(){
        server.stop(0);
    }
}
