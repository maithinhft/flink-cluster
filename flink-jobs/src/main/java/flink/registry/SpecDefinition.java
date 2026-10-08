package flink.registry;

import flink.compile.Node;
import flink.compile.RawPredNode;
import flink.compile.Values;
import flink.model.ValidatedEvent;

import java.util.Set;

/**
 * Canonical aggregation spec: (source field, filter). Function-independent, so one stored
 * (sum, count, min, max) cell serves SUM/COUNT/AVG/MIN/MAX over the same data.
 * {@code specKey} is a deterministic 64-bit hash of the canonical form, identical on every subtask
 * and across restarts.
 */
public final class SpecDefinition {

    public static final String ALL_EVENTS = "*";

    private final long specKey;
    private final String canonical;
    private final String sourceField;
    private final Node filter;
    private final Set<String> eventTypes;

    public SpecDefinition(long specKey, String canonical, String sourceField, Node filter, Set<String> eventTypes) {
        this.specKey = specKey;
        this.canonical = canonical;
        this.sourceField = sourceField;
        this.filter = filter;
        this.eventTypes = eventTypes;
    }

    public long specKey() {
        return specKey;
    }

    public String canonical() {
        return canonical;
    }

    public String sourceField() {
        return sourceField;
    }

    public Node filter() {
        return filter;
    }

    public Set<String> eventTypes() {
        return eventTypes;
    }

    public boolean matches(ValidatedEvent ev) {
        return FilterEval.test(filter, ev);
    }

    public double extract(ValidatedEvent ev) {
        if (ALL_EVENTS.equals(sourceField)) {
            return 1.0;
        }
        int i = ev.find(sourceField);
        if (i < 0) {
            return Double.NaN;
        }
        return Values.numericValue(ev, i);
    }

    @Override
    public String toString() {
        return "Spec{" + Long.toHexString(specKey) + " " + canonical + '}';
    }

    static final class FilterEval implements flink.compile.EvalContext {
        private static final ThreadLocal<FilterEval> LOCAL = ThreadLocal.withInitial(FilterEval::new);
        private ValidatedEvent ev;

        static boolean test(Node filter, ValidatedEvent ev) {
            if (filter instanceof RawPredNode) {
                return ((RawPredNode) filter).test(ev);
            }
            FilterEval ctx = LOCAL.get();
            ctx.ev = ev;
            return filter.eval(ctx);
        }

        @Override
        public ValidatedEvent event() {
            return ev;
        }

        @Override
        public boolean testAggregate(flink.compile.AggPredNode node) {
            return false;
        }

        @Override
        public Double aggregateValue(flink.compile.AggRef ref) {
            return null;
        }

        @Override
        public void record(String path, Double value) {
        }
    }
}
