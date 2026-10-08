package flink.compile;

import flink.registry.SpecDefinition;

/**
 * Reference to a windowed aggregate: either an inline spec (field + filter, derived from the rule)
 * or a named metric alias declared in a schema's {@code metrics[]}. Alias references are resolved
 * against the registry at evaluation time so schemas may arrive after rules.
 */
public final class AggRef {

    private final SpecDefinition spec;
    private final String alias;
    private final AggFunction function;
    private final WindowPlan window;
    private String path = "";

    public AggRef(SpecDefinition spec, String alias, AggFunction function, WindowPlan window) {
        this.spec = spec;
        this.alias = alias;
        this.function = function;
        this.window = window;
    }

    public SpecDefinition spec() {
        return spec;
    }

    public String alias() {
        return alias;
    }

    public AggFunction function() {
        return function;
    }

    public WindowPlan window() {
        return window;
    }

    public String path() {
        return path;
    }

    void assignPath(String p) {
        this.path = p;
    }

    public int cost() {
        return 10 + window.lookbackBuckets();
    }
}
