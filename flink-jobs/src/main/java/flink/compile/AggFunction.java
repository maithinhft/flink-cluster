package flink.compile;

/**
 * Aggregate functions. All five are computed from the same (sum, count, min, max) accumulator.
 * On an empty window SUM and COUNT are 0, while AVG, MIN and MAX are undefined (null) so that
 * any comparison against them is false.
 */
public enum AggFunction {
    SUM, COUNT, AVG, MIN, MAX;

    public static AggFunction parse(String raw) {
        if (raw == null) {
            return COUNT;
        }
        try {
            return valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new CompileException("Unsupported aggregate function: " + raw);
        }
    }

    public Double value(double sum, long count, double min, double max) {
        switch (this) {
            case SUM:
                return count == 0 ? 0.0 : sum;
            case COUNT:
                return (double) count;
            case AVG:
                return count == 0 ? null : sum / count;
            case MIN:
                return count == 0 ? null : min;
            case MAX:
                return count == 0 ? null : max;
            default:
                return null;
        }
    }
}
