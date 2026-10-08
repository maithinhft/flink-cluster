package flink.engine;

/**
 * Monotonic counters exposed as Flink gauges (Prometheus computes rates).
 */
public final class EngineStats {
    public long eventsProcessed;
    public long eventsLate;
    public long specUpdates;
    public long rulesConsidered;
    public long candidatesAfterTrigger;
    public long candidatesAfterIndex;
    public long rulesEvaluated;
    public long rulesMatched;
    public long suppressedByCooldown;
    public long evalErrors;
    public long entitiesCleared;
    public long bucketsPurged;
}
