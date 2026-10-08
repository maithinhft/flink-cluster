package flink.compile;

import com.fasterxml.jackson.databind.JsonNode;
import flink.state.TimeBuckets;

/**
 * Compiled window semantics (spec §4.4), expressed in 5-minute bucket units. Reads never go past the
 * bucket of the triggering event, so out-of-order "future" data does not leak into a decision.
 *
 * <ul>
 *   <li>LOOKBACK (no type): buckets [tb - lookback + 1, tb].</li>
 *   <li>TUMBLING: current tumbling window [floor(tb/size)*size, tb]; with lookback &gt; size the
 *       previous complete tumbling windows inside the lookback are also candidates.</li>
 *   <li>SLIDING: latest window ending at the next slide boundary, capped at tb; with lookback &gt; size,
 *       earlier windows stepping back by slide that fit inside the lookback are also candidates.</li>
 * </ul>
 * Quantifier LATEST evaluates only the latest window; ANY requires one window to satisfy; ALL requires all.
 */
public final class WindowPlan {

    public enum Mode { LOOKBACK, TUMBLING, SLIDING }

    public enum Quantifier { LATEST, ANY, ALL }

    public static final long MAX_SECONDS = TimeBuckets.MAX_WINDOW_MS / 1000L;
    private static final long BUCKET_SECONDS = TimeBuckets.BUCKET_MS / 1000L;

    private final Mode mode;
    private final int sizeB;
    private final int slideB;
    private final int lookB;
    private final Quantifier quantifier;

    public WindowPlan(Mode mode, int sizeB, int slideB, int lookB, Quantifier quantifier) {
        this.mode = mode;
        this.sizeB = sizeB;
        this.slideB = slideB;
        this.lookB = lookB;
        this.quantifier = quantifier;
    }

    public static WindowPlan lookback(int buckets) {
        return new WindowPlan(Mode.LOOKBACK, buckets, 1, buckets, Quantifier.LATEST);
    }

    public static WindowPlan parse(JsonNode win, long defaultLookbackSeconds) {
        if (win == null || win.isNull() || win.isMissingNode() || !win.isObject()) {
            int b = toBuckets(defaultLookbackSeconds, "lookback_seconds");
            return lookback(b);
        }
        String type = win.has("type") ? win.get("type").asText("").trim().toLowerCase() : "";
        long size = seconds(win, "size_seconds");
        long slide = seconds(win, "slide_seconds");
        long look = seconds(win, "lookback_seconds");

        Mode mode;
        if ("tumbling".equals(type)) {
            mode = Mode.TUMBLING;
        } else if ("sliding".equals(type) || "hopping".equals(type)) {
            mode = Mode.SLIDING;
        } else if (type.isEmpty() || "lookback".equals(type)) {
            mode = Mode.LOOKBACK;
        } else {
            throw new CompileException("Unsupported window type: " + type);
        }

        if (mode == Mode.LOOKBACK) {
            long l = look > 0 ? look : (size > 0 ? size : defaultLookbackSeconds);
            int b = toBuckets(l, "lookback_seconds");
            return new WindowPlan(Mode.LOOKBACK, b, 1, b, parseQuantifier(win, Quantifier.LATEST));
        }

        if (size <= 0) {
            size = look > 0 ? look : defaultLookbackSeconds;
        }
        if (mode == Mode.TUMBLING) {
            slide = size;
        } else if (slide <= 0) {
            slide = BUCKET_SECONDS;
        }
        if (look <= 0 || look < size) {
            look = size;
        }
        int sizeB = toBuckets(size, "size_seconds");
        int slideB = toBuckets(slide, "slide_seconds");
        int lookB = toBuckets(look, "lookback_seconds");
        if (slideB > sizeB) {
            throw new CompileException("slide_seconds must not exceed size_seconds");
        }
        Quantifier def = lookB > sizeB ? Quantifier.ANY : Quantifier.LATEST;
        return new WindowPlan(mode, sizeB, slideB, lookB, parseQuantifier(win, def));
    }

    private static Quantifier parseQuantifier(JsonNode win, Quantifier def) {
        if (win.has("quantifier")) {
            try {
                return Quantifier.valueOf(win.get("quantifier").asText().trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new CompileException("Unsupported quantifier: " + win.get("quantifier").asText());
            }
        }
        return def;
    }

    private static long seconds(JsonNode win, String field) {
        if (!win.has(field) || win.get(field).isNull()) {
            return 0L;
        }
        JsonNode n = win.get(field);
        if (!n.canConvertToLong() && !n.isTextual()) {
            throw new CompileException(field + " must be an integer");
        }
        return n.isTextual() ? Long.parseLong(n.asText().trim()) : n.asLong();
    }

    private static int toBuckets(long secondsValue, String field) {
        if (secondsValue <= 0 || secondsValue % BUCKET_SECONDS != 0) {
            throw new CompileException(field + " must be a positive multiple of " + BUCKET_SECONDS + "s, got " + secondsValue);
        }
        if (secondsValue > MAX_SECONDS) {
            throw new CompileException(field + " must be <= " + MAX_SECONDS + "s, got " + secondsValue);
        }
        return (int) (secondsValue / BUCKET_SECONDS);
    }

    public Mode mode() {
        return mode;
    }

    public Quantifier quantifier() {
        return quantifier;
    }

    public int sizeBuckets() {
        return sizeB;
    }

    public int slideBuckets() {
        return slideB;
    }

    public int lookbackBuckets() {
        return lookB;
    }

    public long horizonStart(long tb) {
        return tb - lookB + 1;
    }

    /**
     * Candidate windows for an event in bucket {@code tb}, flattened as [start0, end0, start1, end1, ...]
     * (inclusive bucket indexes, latest window first).
     */
    public long[] windows(long tb) {
        long horizon = horizonStart(tb);
        if (mode == Mode.LOOKBACK) {
            return new long[]{horizon, tb};
        }
        long latestStart;
        long latestEnd;
        if (mode == Mode.TUMBLING) {
            latestStart = Math.floorDiv(tb, (long) sizeB) * sizeB;
            latestEnd = tb;
        } else {
            long endExclusive = Math.floorDiv(tb, (long) slideB) * slideB + slideB;
            latestStart = endExclusive - sizeB;
            latestEnd = Math.min(endExclusive - 1, tb);
        }
        if (quantifier == Quantifier.LATEST) {
            return new long[]{latestStart, latestEnd};
        }
        long step = mode == Mode.TUMBLING ? sizeB : slideB;
        int maxWindows = 1 + (int) Math.max(0, (latestStart - horizon) / step);
        long[] out = new long[maxWindows * 2];
        int n = 0;
        out[n++] = latestStart;
        out[n++] = latestEnd;
        long start = latestStart - step;
        while (start >= horizon && n < out.length) {
            out[n++] = start;
            out[n++] = start + sizeB - 1;
            start -= step;
        }
        if (n == out.length) {
            return out;
        }
        long[] trimmed = new long[n];
        System.arraycopy(out, 0, trimmed, 0, n);
        return trimmed;
    }

    public long earliestBucket(long tb) {
        long[] w = windows(tb);
        long min = w[0];
        for (int i = 0; i < w.length; i += 2) {
            min = Math.min(min, w[i]);
        }
        return min;
    }

    public long describeKey() {
        return ((long) mode.ordinal() << 60) ^ ((long) quantifier.ordinal() << 56)
                ^ ((long) sizeB << 36) ^ ((long) slideB << 18) ^ lookB;
    }

    @Override
    public String toString() {
        return mode + "(size=" + sizeB + ",slide=" + slideB + ",lookback=" + lookB + "," + quantifier + ")";
    }
}
