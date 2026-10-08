package flink.compile;

import flink.model.ValidatedEvent;

/**
 * Per-event evaluation context: gives nodes access to the triggering event and to windowed aggregates
 * of the current entity. Implementations memoise aggregate reads within one event.
 */
public interface EvalContext {

    ValidatedEvent event();

    boolean testAggregate(AggPredNode node);

    Double aggregateValue(AggRef ref);

    void record(String path, Double value);
}
