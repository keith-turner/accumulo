package org.apache.accumulo.test.tracing;

import java.util.Map;

public class SpanData {

    public final String traceId;
    public final String name;
    public final Map<String, String> stringAttributes;
    public final Map<String, Long> integerAttributes;

    public SpanData(String traceId, String name, Map<String, String> stringAttributes, Map<String, Long> integerAttributes) {
        this.traceId = traceId;
        this.name = name;
        this.stringAttributes = stringAttributes;
        this.integerAttributes = integerAttributes;
    }

    @Override
    public String toString() {
        return "SpanData{" +
                "traceId='" + traceId + '\'' +
                ", name='" + name + '\'' +
                ", stringAttributes=" + stringAttributes +
                ", integerAttributes=" + integerAttributes +
                '}';
    }
}
