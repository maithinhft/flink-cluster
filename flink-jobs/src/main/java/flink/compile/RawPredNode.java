package flink.compile;

import flink.model.ValidatedEvent;

import java.util.Collections;
import java.util.Set;

/**
 * Leaf condition on a field of the triggering event. A missing/null field only satisfies the
 * negative operators (NEQ, NOT_IN).
 */
public final class RawPredNode extends Node {

    private final String field;
    private final Op op;
    private final Operand operand;
    private final Operand lo;
    private final Operand hi;
    private final Set<String> inSet;

    public RawPredNode(String field, Op op, Operand operand, Operand lo, Operand hi, Set<String> inSet) {
        this.field = field;
        this.op = op;
        this.operand = operand;
        this.lo = lo;
        this.hi = hi;
        this.inSet = inSet == null ? Collections.emptySet() : inSet;
    }

    public String field() {
        return field;
    }

    public Op op() {
        return op;
    }

    public Operand operand() {
        return operand;
    }

    public Set<String> inValues() {
        return inSet;
    }

    @Override
    public int cost() {
        return 1;
    }

    @Override
    public boolean eval(EvalContext ctx) {
        return test(ctx.event());
    }

    public boolean test(ValidatedEvent ev) {
        int i = ev.find(field);
        if (i < 0 || ev.kind(i) == ValidatedEvent.KIND_NULL) {
            return op.isNegative();
        }
        switch (op) {
            case EQ:
                return operand.canon != null && operand.canon.equals(Values.canonEvent(ev, i));
            case NEQ:
                return operand.canon == null || !operand.canon.equals(Values.canonEvent(ev, i));
            case IN:
                return inSet.contains(Values.canonEvent(ev, i));
            case NOT_IN:
                return !inSet.contains(Values.canonEvent(ev, i));
            case CONTAINS: {
                String s = Values.canonEvent(ev, i);
                return s != null && operand.canon != null && s.contains(operand.canon);
            }
            case BETWEEN: {
                Integer a = compare(ev, i, lo);
                Integer b = compare(ev, i, hi);
                return a != null && b != null && a >= 0 && b <= 0;
            }
            default: {
                Integer c = compare(ev, i, operand);
                return c != null && op.testCompare(c);
            }
        }
    }

    static Integer compare(ValidatedEvent ev, int i, Operand o) {
        byte kind = ev.kind(i);
        if (kind == ValidatedEvent.KIND_NUMBER || kind == ValidatedEvent.KIND_BOOLEAN) {
            if (!Double.isNaN(o.num)) {
                return Double.compare(ev.num(i), o.num);
            }
            if (o.ts != Values.NO_TIMESTAMP) {
                return Double.compare(ev.num(i), (double) o.ts);
            }
            return null;
        }
        String s = ev.str(i);
        if (o.ts != Values.NO_TIMESTAMP) {
            long t = Values.parseTimestamp(s);
            if (t != Values.NO_TIMESTAMP) {
                return Long.compare(t, o.ts);
            }
        }
        if (!Double.isNaN(o.num)) {
            double d = Values.parseDouble(s);
            if (!Double.isNaN(d)) {
                return Double.compare(d, o.num);
            }
            return null;
        }
        if (o.canon == null) {
            return null;
        }
        return s.compareTo(o.canon);
    }
}
