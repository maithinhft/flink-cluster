package flink.testutil;

import flink.control.ControlMessageParser;
import flink.engine.EngineConfig;
import flink.engine.EntityProcessor;
import flink.model.ControlMessage;
import flink.model.KeyedRecord;
import flink.model.ValidatedEvent;
import flink.state.CompactBucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class TestSupport {

    public static final long T0 = 1_700_000_100_000L - (1_700_000_100_000L % 300_000L);

    private TestSupport() {
    }

    public static EngineConfig config() {
        EngineConfig c = new EngineConfig();
        c.bootstrapEnabled = false;
        c.stateTtlHours = 0;
        return c;
    }

    public static ControlMessage rule(String id, long version, String ruleJson, long cooldownSeconds) {
        return rule(id, version, ruleJson, cooldownSeconds, true, Long.MIN_VALUE);
    }

    public static ControlMessage rule(String id, long version, String ruleJson, long cooldownSeconds,
                                      boolean enabled, long updatedAtMs) {
        String row = ControlMessageParser.ruleRow(id, id + "-name", ruleJson, cooldownSeconds, version, enabled,
                0L, updatedAtMs == Long.MIN_VALUE ? 0L : updatedAtMs).toString();
        ControlMessage m = ControlMessageParser.parse(row);
        if (updatedAtMs == Long.MIN_VALUE) {
            m.effectiveFromMs = Long.MIN_VALUE;
        }
        return m;
    }

    public static ControlMessage deleteRule(String id, long version) {
        return new ControlMessage(ControlMessage.Kind.RULE_DELETE, id, version, Long.toString(version),
                Long.MIN_VALUE, 0L, "{\"rule_id\":\"" + id + "\"}");
    }

    public static ControlMessage schema(String id, String version, String payload) {
        return new ControlMessage(ControlMessage.Kind.SCHEMA_UPSERT, id, 0L, version, Long.MIN_VALUE, 0L, payload);
    }

    public static ValidatedEvent.Builder event(String entity, String type, long timeMs) {
        return ValidatedEvent.builder()
                .entity(entity)
                .eventId(entity + "-" + type + "-" + timeMs)
                .eventType(type)
                .source("ecommerce")
                .time(timeMs)
                .str("event_type", type);
    }

    public static final class MemStore implements EntityProcessor.BucketStore {
        public final Map<Integer, CompactBucket> map = new TreeMap<>();
        public int gets;
        public int scans;

        @Override
        public CompactBucket get(int slot) {
            gets++;
            CompactBucket b = map.get(slot);
            return b == null ? null : b.copy();
        }

        @Override
        public void put(int slot, CompactBucket bucket) {
            map.put(slot, bucket.copy());
        }

        @Override
        public void remove(int slot) {
            map.remove(slot);
        }

        @Override
        public void forEach(EntityProcessor.SlotConsumer consumer) throws Exception {
            scans++;
            for (Map.Entry<Integer, CompactBucket> e : new TreeMap<>(map).entrySet()) {
                consumer.accept(e.getKey(), e.getValue().copy());
            }
        }

        public void resetCounters() {
            gets = 0;
            scans = 0;
        }
    }

    public static final class MemCooldowns implements EntityProcessor.CooldownStore {
        public final Map<String, Long> map = new HashMap<>();

        @Override
        public Long get(String ruleId) {
            return map.get(ruleId);
        }

        @Override
        public void put(String ruleId, long untilMs) {
            map.put(ruleId, untilMs);
        }
    }

    public static final class Alerts implements EntityProcessor.AlertOutput {
        public final List<KeyedRecord> records = new ArrayList<>();

        @Override
        public void emit(KeyedRecord alert) {
            records.add(alert);
        }

        public int size() {
            return records.size();
        }

        public void clear() {
            records.clear();
        }
    }

    public static final class Entity {
        public final MemStore store = new MemStore();
        public final MemCooldowns cooldowns = new MemCooldowns();
        public final Alerts alerts = new Alerts();
        private final EntityProcessor processor;

        public Entity(EntityProcessor processor) {
            this.processor = processor;
        }

        public String send(ValidatedEvent ev) throws Exception {
            return processor.process(ev, Long.MIN_VALUE, ev.eventTimeMs, store, cooldowns, alerts);
        }

        public String send(ValidatedEvent ev, long watermark) throws Exception {
            return processor.process(ev, watermark, ev.eventTimeMs, store, cooldowns, alerts);
        }
    }
}
