package flink.compile;

/**
 * Leaf condition comparing a windowed aggregate of the entity against a threshold.
 */
public final class AggPredNode extends Node {

    private final AggRef ref;
    private final Op op;
    private final double threshold;
    private final double lo;
    private final double hi;

    public AggPredNode(AggRef ref, Op op, double threshold, double lo, double hi) {
        this.ref = ref;
        this.op = op;
        this.threshold = threshold;
        this.lo = lo;
        this.hi = hi;
    }

    public AggRef ref() {
        return ref;
    }

    public boolean test(Double value) {
        if (value == null) {
            return false;
        }
        return op.testNumeric(value, threshold, lo, hi);
    }

    @Override
    void assignPath(String p) {
        super.assignPath(p);
        ref.assignPath(p);
    }

    @Override
    public boolean eval(EvalContext ctx) {
        return ctx.testAggregate(this);
    }

    @Override
    public int cost() {
        return ref.cost();
    }
}
