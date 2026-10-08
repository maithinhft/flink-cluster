package flink.engine;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import flink.compile.CompiledRule;
import flink.model.KeyedRecord;
import flink.model.ValidatedEvent;
import flink.state.TimeBuckets;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the alert JSON written to the {@code result} topic. {@code alert_id} is deterministic
 * (UUIDv3 of entity|rule|version|event), so replays after a failover produce the same id and the sink can
 * upsert idempotently.
 */
public final class AlertBuilder {

    private static final JsonFactory FACTORY = new JsonFactory();

    private final boolean includePayload;

    public AlertBuilder(boolean includePayload) {
        this.includePayload = includePayload;
    }

    public static String alertId(String entityId, String ruleId, long version, String eventId) {
        String seed = entityId + "|" + ruleId + "|" + version + "|" + eventId;
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    public KeyedRecord build(ValidatedEvent ev, CompiledRule rule, Map<String, Double> evaluated,
                             boolean windowComplete, long cooldownUntilMs, long lateToleranceMs, long nowMs) {
        long bucketStart = TimeBuckets.bucketStart(ev.eventTimeMs);
        StringWriter w = new StringWriter(512);
        try (JsonGenerator g = FACTORY.createGenerator(w)) {
            g.writeStartObject();
            g.writeStringField("alert_id", alertId(ev.entityId, rule.ruleId(), rule.version(), ev.eventId));
            g.writeStringField("entity_id", ev.entityId);
            g.writeStringField("rule_id", rule.ruleId());
            g.writeStringField("rule_name", rule.name());
            g.writeNumberField("rule_version", rule.version());
            g.writeStringField("triggered_at", Instant.ofEpochMilli(nowMs).toString());
            g.writeNumberField("triggered_at_ms", nowMs);
            g.writeStringField("event_time", Instant.ofEpochMilli(ev.eventTimeMs).toString());
            g.writeNumberField("event_time_ms", ev.eventTimeMs);
            g.writeBooleanField("window_complete", windowComplete);

            g.writeObjectFieldStart("evaluated_values");
            for (Map.Entry<String, Double> e : evaluated.entrySet()) {
                if (e.getValue() == null) {
                    g.writeNullField(e.getKey());
                } else {
                    g.writeNumberField(e.getKey(), e.getValue());
                }
            }
            g.writeEndObject();

            g.writeObjectFieldStart("trigger_event");
            g.writeStringField("event_id", ev.eventId);
            g.writeStringField("event_type", ev.eventType);
            g.writeStringField("source_system", ev.sourceSystem);
            if (includePayload && ev.rawJson != null) {
                g.writeFieldName("payload");
                g.writeRawValue(ev.rawJson);
            }
            g.writeEndObject();

            g.writeObjectFieldStart("audit");
            g.writeNumberField("cooldown_seconds", rule.cooldownMs() / 1000L);
            if (cooldownUntilMs > 0) {
                g.writeStringField("cooldown_until", Instant.ofEpochMilli(cooldownUntilMs).toString());
            } else {
                g.writeNullField("cooldown_until");
            }
            g.writeNumberField("cooldown_until_ms", cooldownUntilMs);
            g.writeNumberField("bucket_start_ms", bucketStart);
            g.writeNumberField("bucket_end_ms", bucketStart + TimeBuckets.BUCKET_MS);
            g.writeNumberField("bucket_close_time_ms", bucketStart + TimeBuckets.BUCKET_MS + lateToleranceMs);
            g.writeEndObject();

            g.writeObjectFieldStart("latency");
            g.writeNumberField("ingest_time_ms", ev.ingestTimeMs);
            g.writeNumberField("emit_time_ms", nowMs);
            g.writeEndObject();

            g.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new KeyedRecord(ev.entityId, w.toString());
    }
}
