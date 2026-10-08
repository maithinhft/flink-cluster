package flink.compile;

/**
 * Comparison operators supported in leaf conditions. Accepts both symbolic ("&gt;=") and named ("GTE") forms.
 */
public enum Op {
    EQ, NEQ, GT, GTE, LT, LTE, IN, NOT_IN, BETWEEN, CONTAINS;

    public static Op parse(String raw) {
        if (raw == null) {
            return EQ;
        }
        String s = raw.trim().toUpperCase().replace(' ', '_');
        switch (s) {
            case "EQ":
            case "=":
            case "==":
                return EQ;
            case "NEQ":
            case "NE":
            case "!=":
            case "<>":
                return NEQ;
            case "GT":
            case ">":
                return GT;
            case "GTE":
            case "GE":
            case ">=":
                return GTE;
            case "LT":
            case "<":
                return LT;
            case "LTE":
            case "LE":
            case "<=":
                return LTE;
            case "IN":
                return IN;
            case "NOT_IN":
            case "NIN":
                return NOT_IN;
            case "BETWEEN":
                return BETWEEN;
            case "CONTAINS":
                return CONTAINS;
            default:
                throw new CompileException("Unsupported operator: " + raw);
        }
    }

    public boolean isNegative() {
        return this == NEQ || this == NOT_IN;
    }

    public boolean isOrdering() {
        return this == GT || this == GTE || this == LT || this == LTE;
    }

    public boolean testCompare(int cmp) {
        switch (this) {
            case EQ:
                return cmp == 0;
            case NEQ:
                return cmp != 0;
            case GT:
                return cmp > 0;
            case GTE:
                return cmp >= 0;
            case LT:
                return cmp < 0;
            case LTE:
                return cmp <= 0;
            default:
                return false;
        }
    }

    public boolean testNumeric(double actual, double threshold, double lo, double hi) {
        if (this == BETWEEN) {
            return actual >= lo && actual <= hi;
        }
        return testCompare(Double.compare(actual, threshold));
    }
}
