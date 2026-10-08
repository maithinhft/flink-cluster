package flink.compile;

import flink.model.ValidatedEvent;

/**
 * Linear expression {@code sum(coef_i * term_i) + constant OP value}, where each term is either a raw
 * numeric field of the triggering event or a windowed aggregate. Any undefined term makes the whole
 * predicate false.
 */
public final class LinearPredNode extends Node {

    private final double[] coefs;
    private final String[] rawFields;
    private final AggRef[] aggTerms;
    private final double constant;
    private final Op op;
    private final double threshold;
    private final double lo;
    private final double hi;
    private final int cost;

    public LinearPredNode(double[] coefs, String[] rawFields, AggRef[] aggTerms, double constant,
                          Op op, double threshold, double lo, double hi) {
        this.coefs = coefs;
        this.rawFields = rawFields;
        this.aggTerms = aggTerms;
        this.constant = constant;
        this.op = op;
        this.threshold = threshold;
        this.lo = lo;
        this.hi = hi;
        int c = 1;
        for (AggRef r : aggTerms) {
            if (r != null) {
                c += r.cost();
            }
        }
        this.cost = c;
    }

    public AggRef[] aggTerms() {
        return aggTerms;
    }

    @Override
    void assignPath(String p) {
        super.assignPath(p);
        for (int i = 0; i < aggTerms.length; i++) {
            if (aggTerms[i] != null) {
                aggTerms[i].assignPath(p + ".t" + i);
            }
        }
    }

    @Override
    public boolean eval(EvalContext ctx) {
        ValidatedEvent ev = ctx.event();
        double total = constant;
        for (int i = 0; i < coefs.length; i++) {
            double v;
            if (aggTerms[i] != null) {
                Double a = ctx.aggregateValue(aggTerms[i]);
                if (a == null) {
                    return false;
                }
                v = a;
            } else {
                int idx = ev.find(rawFields[i]);
                if (idx < 0) {
                    return false;
                }
                v = Values.numericValue(ev, idx);
                if (Double.isNaN(v)) {
                    return false;
                }
            }
            total += coefs[i] * v;
        }
        ctx.record(path, total);
        return op.testNumeric(total, threshold, lo, hi);
    }

    @Override
    public int cost() {
        return cost;
    }
}
