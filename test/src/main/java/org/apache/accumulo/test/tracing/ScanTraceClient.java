package org.apache.accumulo.test.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.data.Range;

import java.util.List;

public class ScanTraceClient {
    public static void main(String[] args) throws Exception {
        String clientPropsPath = args[0];
        String table = args[1];

        Tracer tracer = GlobalOpenTelemetry.get().getTracer(ScanTraceClient.class.getName());
        Span span = tracer.spanBuilder("test-scan").startSpan();

        System.out.println(span.getSpanContext().getTraceId());

        try(var client = Accumulo.newClient().from(clientPropsPath).build(); var scope = span.makeCurrent()) {
            try(var scanner = client.createBatchScanner(table)) {
                scanner.setRanges(List.of(new Range()));
                System.out.println(scanner.stream().count());
            }
            try(var scanner = client.createScanner(table)) {
                scanner.setBatchSize(10_000);
                System.out.println(scanner.stream().count());
            }
        }finally {
            span.end();
        }

    }
}
