package flink.model;

/**
 * Unified control-plane message (rule or schema change) broadcast to every parallel instance.
 * Produced from Debezium CDC records or from the PostgreSQL bootstrap snapshot.
 */
public class ControlMessage {

    public enum Kind { RULE_UPSERT, RULE_DELETE, SCHEMA_UPSERT, SCHEMA_DELETE }

    public Kind kind;
    public String id;
    public long version;
    public String versionLabel;
    public long effectiveFromMs;
    public long sourceTsMs;
    public String payload;

    public ControlMessage() {
    }

    public ControlMessage(Kind kind, String id, long version, String versionLabel,
                          long effectiveFromMs, long sourceTsMs, String payload) {
        this.kind = kind;
        this.id = id;
        this.version = version;
        this.versionLabel = versionLabel;
        this.effectiveFromMs = effectiveFromMs;
        this.sourceTsMs = sourceTsMs;
        this.payload = payload;
    }

    public boolean isRule() {
        return kind == Kind.RULE_UPSERT || kind == Kind.RULE_DELETE;
    }

    public boolean isDelete() {
        return kind == Kind.RULE_DELETE || kind == Kind.SCHEMA_DELETE;
    }

    @Override
    public String toString() {
        return "ControlMessage{" + kind + ", id=" + id + ", version=" + version + '}';
    }
}
