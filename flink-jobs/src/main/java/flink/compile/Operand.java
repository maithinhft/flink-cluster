package flink.compile;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A rule constant pre-converted into every representation a comparison may need,
 * so nothing is parsed per event.
 */
public final class Operand {

    final String canon;
    final double num;
    final long ts;

    private Operand(String canon, double num, long ts) {
        this.canon = canon;
        this.num = num;
        this.ts = ts;
    }

    public static Operand of(JsonNode n) {
        String canon = Values.canonJson(n);
        double num = Values.toDouble(n);
        long ts = n != null && n.isTextual() ? Values.parseTimestamp(n.asText()) : Values.NO_TIMESTAMP;
        return new Operand(canon, num, ts);
    }

    public String canon() {
        return canon;
    }

    public double num() {
        return num;
    }
}
