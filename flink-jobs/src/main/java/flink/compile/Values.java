package flink.compile;

import com.fasterxml.jackson.databind.JsonNode;
import flink.model.ValidatedEvent;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Value normalisation shared by the evaluator, the canonical spec hashing and the inverted index.
 * Equality is defined on the canonical string form, so the index and the evaluator always agree
 * (e.g. event value 5.0 equals rule constant 5 or "5").
 */
public final class Values {

    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    private Values() {
    }

    public static String canonNumber(double d) {
        if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }

    public static String canonEvent(ValidatedEvent ev, int i) {
        switch (ev.kind(i)) {
            case ValidatedEvent.KIND_STRING:
                return ev.str(i);
            case ValidatedEvent.KIND_NUMBER:
                return canonNumber(ev.num(i));
            case ValidatedEvent.KIND_BOOLEAN:
                return ev.num(i) != 0.0 ? "true" : "false";
            default:
                return null;
        }
    }

    public static String canonJson(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) {
            return null;
        }
        if (n.isNumber()) {
            return canonNumber(n.asDouble());
        }
        if (n.isBoolean()) {
            return n.asBoolean() ? "true" : "false";
        }
        if (n.isValueNode()) {
            return n.asText();
        }
        return n.toString();
    }

    public static double toDouble(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) {
            return Double.NaN;
        }
        if (n.isNumber()) {
            return n.asDouble();
        }
        if (n.isTextual()) {
            return parseDouble(n.asText());
        }
        return Double.NaN;
    }

    public static double parseDouble(String s) {
        if (s == null) {
            return Double.NaN;
        }
        try {
            return Double.parseDouble(s.trim());
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    public static long parseTimestamp(String s) {
        if (s == null || s.length() < 10) {
            return NO_TIMESTAMP;
        }
        char c4 = s.charAt(4);
        if (c4 != '-') {
            return NO_TIMESTAMP;
        }
        try {
            return Instant.parse(s).toEpochMilli();
        } catch (Exception ignored) {
        }
        try {
            return OffsetDateTime.parse(s).toInstant().toEpochMilli();
        } catch (Exception ignored) {
        }
        if (s.length() == 10) {
            try {
                return LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (Exception ignored) {
            }
        }
        return NO_TIMESTAMP;
    }

    public static double numericValue(ValidatedEvent ev, int i) {
        switch (ev.kind(i)) {
            case ValidatedEvent.KIND_NUMBER:
                return ev.num(i);
            case ValidatedEvent.KIND_STRING: {
                String s = ev.str(i);
                long ts = parseTimestamp(s);
                if (ts != NO_TIMESTAMP) {
                    return ts;
                }
                return parseDouble(s);
            }
            default:
                return Double.NaN;
        }
    }
}
