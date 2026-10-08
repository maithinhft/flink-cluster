package flink.engine;

import flink.index.InvertedIndex;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

/**
 * Tunables of the segmentation engine, read from job parameters.
 */
public final class EngineConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum EffectiveMode { EVENT_TIME, IMMEDIATE }

    public InvertedIndex.Mode indexMode = InvertedIndex.Mode.DF2;
    public EffectiveMode effectiveMode = EffectiveMode.EVENT_TIME;
    public boolean includePayload = false;
    public long lateToleranceMs = 60_000L;
    public int fullLoadThreshold = 24;
    public long retiredRetentionMs = 25L * 3600_000L;
    public int stateTtlHours = 25;
    public boolean projectionEnabled = true;
    public long schemaGraceMs = 5L * 60_000L;
    public long coldStartBufferMs = 30_000L;
    public int coldStartBufferMax = 100_000;
    public boolean bootstrapEnabled = false;
    public String bootstrapUrl;
    public String bootstrapUser;
    public String bootstrapPassword;

    public static EngineConfig fromParameters(ParameterTool p) {
        EngineConfig c = new EngineConfig();
        boolean indexEnabled = p.getBoolean("index.enabled", true);
        String mode = p.get("index.mode", indexEnabled ? "df2" : "full").trim().toUpperCase().replace('-', '_');
        if ("TRIGGER_ONLY".equals(mode)) {
            mode = "TRIGGER";
        }
        c.indexMode = InvertedIndex.Mode.valueOf(mode);
        c.effectiveMode = EffectiveMode.valueOf(
                p.get("definition.effective.mode", "event-time").trim().toUpperCase().replace('-', '_'));
        c.includePayload = p.getBoolean("output.include.payload", false);
        c.lateToleranceMs = p.getLong("late.tolerance.ms", c.lateToleranceMs);
        c.fullLoadThreshold = p.getInt("state.full.load.threshold", c.fullLoadThreshold);
        c.retiredRetentionMs = p.getLong("registry.retired.retention.ms", c.retiredRetentionMs);
        c.stateTtlHours = p.getInt("state.ttl.hours", c.stateTtlHours);
        c.projectionEnabled = p.getBoolean("projection.enabled", true);
        c.schemaGraceMs = p.getLong("schema.grace.ms", c.schemaGraceMs);
        c.coldStartBufferMs = p.getLong("coldstart.buffer.ms", c.coldStartBufferMs);
        c.coldStartBufferMax = p.getInt("coldstart.buffer.max", c.coldStartBufferMax);
        c.bootstrapEnabled = p.getBoolean("bootstrap.enabled", true);
        String host = p.get("postgres.host", "postgres");
        String port = p.get("postgres.port", "5432");
        String db = p.get("postgres.db", "realtime_core");
        c.bootstrapUrl = p.get("postgres.url", "jdbc:postgresql://" + host + ":" + port + "/" + db);
        c.bootstrapUser = p.get("postgres.user", "postgres");
        c.bootstrapPassword = p.get("postgres.password", "postgres");
        return c;
    }
}
