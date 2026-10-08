package flink.compile;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WindowPlanTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static WindowPlan parse(String json) throws Exception {
        return WindowPlan.parse(M.readTree(json), 300);
    }

    @Test
    void lookbackCoversLastNBucketsIncludingCurrent() throws Exception {
        WindowPlan p = parse("{\"lookback_seconds\":3600}");
        assertEquals(WindowPlan.Mode.LOOKBACK, p.mode());
        assertArrayEquals(new long[]{89, 100}, p.windows(100));
    }

    @Test
    void missingWindowDefaultsToOneBucket() {
        WindowPlan p = WindowPlan.parse(null, 300);
        assertArrayEquals(new long[]{100, 100}, p.windows(100));
    }

    @Test
    void tumblingUsesCurrentWindowCappedAtEventBucket() throws Exception {
        WindowPlan p = parse("{\"type\":\"tumbling\",\"size_seconds\":3600}");
        assertEquals(WindowPlan.Quantifier.LATEST, p.quantifier());
        assertArrayEquals(new long[]{96, 101}, p.windows(101));
    }

    @Test
    void slidingLatestWindowEndsAtEventBucket() throws Exception {
        WindowPlan p = parse("{\"type\":\"sliding\",\"size_seconds\":900,\"lookback_seconds\":900}");
        assertArrayEquals(new long[]{98, 100}, p.windows(100));
    }

    @Test
    void lookbackLongerThanSizeDefaultsToAnyOverEarlierWindows() throws Exception {
        WindowPlan p = parse("{\"type\":\"tumbling\",\"size_seconds\":3600,\"lookback_seconds\":10800}");
        assertEquals(WindowPlan.Quantifier.ANY, p.quantifier());
        assertArrayEquals(new long[]{24, 25, 12, 23, 0, 11}, p.windows(25));

        WindowPlan s = parse("{\"type\":\"sliding\",\"size_seconds\":900,\"slide_seconds\":300,\"lookback_seconds\":1500}");
        assertArrayEquals(new long[]{8, 10, 7, 9, 6, 8}, s.windows(10));
    }

    @Test
    void explicitQuantifierIsRespected() throws Exception {
        WindowPlan p = parse("{\"type\":\"tumbling\",\"size_seconds\":3600,\"lookback_seconds\":7200,\"quantifier\":\"all\"}");
        assertEquals(WindowPlan.Quantifier.ALL, p.quantifier());
    }

    @Test
    void rejectsNonMultiplesAndWindowsLongerThan24h() {
        assertThrows(CompileException.class, () -> parse("{\"lookback_seconds\":400}"));
        assertThrows(CompileException.class, () -> parse("{\"lookback_seconds\":86700}"));
        assertThrows(CompileException.class, () -> parse("{\"type\":\"sliding\",\"size_seconds\":600,\"slide_seconds\":900}"));
        assertThrows(CompileException.class, () -> parse("{\"type\":\"session\",\"size_seconds\":600}"));
    }

    @Test
    void fullDayWindowSpans288Buckets() throws Exception {
        WindowPlan p = parse("{\"lookback_seconds\":86400}");
        long[] w = p.windows(1000);
        assertEquals(288, w[1] - w[0] + 1);
    }
}
