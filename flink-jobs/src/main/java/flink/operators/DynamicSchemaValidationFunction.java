package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.control.PostgresControlBootstrap;
import flink.engine.EngineConfig;
import flink.engine.SchemaCatalog;
import flink.io.KafkaIo;
import flink.model.ControlMessage;
import flink.model.KeyedRecord;
import flink.model.RawEvent;
import flink.model.ValidatedEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

public class DynamicSchemaValidationFunction
        extends BroadcastProcessFunction<RawEvent, ControlMessage, ValidatedEvent>
        implements CheckpointedFunction {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicSchemaValidationFunction.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SEP = "\u0001";
    private static final String SCHEMA_NOT_FOUND = "Schema not found for key: ";

    public static final MapStateDescriptor<String, String> SCHEMA_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("validationSchemas", Types.STRING, Types.STRING);

    public static final MapStateDescriptor<String, String> RULE_FIELDS_DESCRIPTOR =
            new MapStateDescriptor<>("validationRuleFields", Types.STRING, Types.STRING);

    public static final OutputTag<KeyedRecord> DIRTY_DATA_TAG = new OutputTag<KeyedRecord>("dirty-events") {
    };

    private final EngineConfig config;

    private transient SchemaCatalog catalog;
    private transient List<ControlMessage> bootstrap;
    private transient List<RawEvent> coldBuffer;
    private transient ListState<RawEvent> coldBufferState;
    private transient long openTimeMs;
    private transient Counter validCounter;
    private transient Counter dirtyCounter;
    private transient Counter bufferedCounter;

    public DynamicSchemaValidationFunction(EngineConfig config) {
        this.config = config;
    }

    public static MapStateDescriptor<?, ?>[] descriptors() {
        return new MapStateDescriptor<?, ?>[]{SCHEMA_STATE_DESCRIPTOR, RULE_FIELDS_DESCRIPTOR};
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        openTimeMs = Long.MIN_VALUE;
        bootstrap = config.bootstrapEnabled
                ? PostgresControlBootstrap.load(config.bootstrapUrl, config.bootstrapUser, config.bootstrapPassword, true, true)
                : Collections.emptyList();
        validCounter = getRuntimeContext().getMetricGroup().counter("events_validated");
        dirtyCounter = getRuntimeContext().getMetricGroup().counter("events_dirty");
        bufferedCounter = getRuntimeContext().getMetricGroup().counter("events_coldstart_buffered");
        getRuntimeContext().getMetricGroup().gauge("schemas_active", () -> catalog == null ? 0 : catalog.size());
        getRuntimeContext().getMetricGroup().gauge("coldstart_buffer_size", () -> coldBuffer == null ? 0 : coldBuffer.size());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        coldBufferState = context.getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("coldStartBuffer", RawEvent.class));
        coldBuffer = new ArrayList<>();
        if (context.isRestored()) {
            for (RawEvent e : coldBufferState.get()) {
                coldBuffer.add(e);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        coldBufferState.update(coldBuffer);
    }

    private void ensureCatalog(ReadOnlyBroadcastState<String, String> schemas,
                               ReadOnlyBroadcastState<String, String> ruleFields) throws Exception {
        if (catalog != null) {
            return;
        }
        catalog = new SchemaCatalog(config.schemaGraceMs);
        long now = System.currentTimeMillis();
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, String> e : schemas.immutableEntries()) {
            sorted.put(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            String[] k = e.getKey().split(SEP, 2);
            if (k.length == 2) {
                catalog.apply(new ControlMessage(ControlMessage.Kind.SCHEMA_UPSERT, k[0], 0L, k[1],
                        Long.MIN_VALUE, 0L, e.getValue()), now);
            }
        }
        for (Map.Entry<String, String> e : ruleFields.immutableEntries()) {
            String[] v = e.getValue().split(SEP, 2);
            Set<String> fields = new HashSet<>();
            if (v.length == 2 && !v[1].isEmpty()) {
                fields.addAll(Arrays.asList(v[1].split(",")));
            }
            catalog.putRuleFields(e.getKey(), fields);
        }
        if (bootstrap != null) {
            for (ControlMessage m : bootstrap) {
                if (!m.isRule() && catalog.has(m.id + "_" + m.versionLabel)) {
                    continue;
                }
                if (m.isRule() && ruleFields.contains(m.id)) {
                    continue;
                }
                catalog.apply(m, now);
            }
            bootstrap = null;
        }
        LOG.info("Validation catalog ready: {} schemas, {} referenced fields", catalog.size(),
                catalog.referencedFields().size());
    }

    @Override
    public void processElement(RawEvent raw, ReadOnlyContext ctx, Collector<ValidatedEvent> out) throws Exception {
        ensureCatalog(ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR), ctx.getBroadcastState(RULE_FIELDS_DESCRIPTOR));
        long now = ctx.currentProcessingTime();
        if (openTimeMs == Long.MIN_VALUE) {
            openTimeMs = now;
        }
        Consumer<KeyedRecord> dirty = r -> ctx.output(DIRTY_DATA_TAG, r);
        if (!coldBuffer.isEmpty()) {
            flushColdBuffer(dirty, out, now - openTimeMs >= config.coldStartBufferMs);
        }
        handle(raw, dirty, out, now - openTimeMs < config.coldStartBufferMs);
    }

    @Override
    public void processBroadcastElement(ControlMessage m, Context ctx, Collector<ValidatedEvent> out) throws Exception {
        BroadcastState<String, String> schemas = ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR);
        BroadcastState<String, String> ruleFields = ctx.getBroadcastState(RULE_FIELDS_DESCRIPTOR);
        ensureCatalog(schemas, ruleFields);
        if (openTimeMs == Long.MIN_VALUE) {
            openTimeMs = ctx.currentProcessingTime();
        }
        long wm = ctx.currentWatermark();
        long clock = wm == Long.MIN_VALUE || wm > Long.MAX_VALUE / 2 ? ctx.currentProcessingTime() : wm;
        if (m.isRule()) {
            applyRule(m, ruleFields);
        } else {
            applySchema(m, schemas, clock);
        }
        if (!coldBuffer.isEmpty()) {
            flushColdBuffer(r -> ctx.output(DIRTY_DATA_TAG, r), out,
                    ctx.currentProcessingTime() - openTimeMs >= config.coldStartBufferMs);
        }
    }

    private void applyRule(ControlMessage m, BroadcastState<String, String> ruleFields) throws Exception {
        String stored = ruleFields.get(m.id);
        long storedVersion = stored == null ? Long.MIN_VALUE : Long.parseLong(stored.split(SEP, 2)[0]);
        if (m.kind == ControlMessage.Kind.RULE_DELETE) {
            if (stored != null && m.version >= storedVersion) {
                ruleFields.remove(m.id);
                catalog.apply(m, 0L);
            }
            return;
        }
        if (m.version <= storedVersion) {
            return;
        }
        catalog.apply(m, 0L);
        Set<String> fields = catalog.ruleFields(m.id);
        ruleFields.put(m.id, m.version + SEP + (fields == null ? "" : String.join(",", fields)));
    }

    private void applySchema(ControlMessage m, BroadcastState<String, String> schemas, long clock) throws Exception {
        if (m.kind == ControlMessage.Kind.SCHEMA_DELETE) {
            List<String> drop = new ArrayList<>();
            for (Map.Entry<String, String> e : schemas.entries()) {
                String[] k = e.getKey().split(SEP, 2);
                if (k[0].equals(m.id) && (m.versionLabel == null || m.versionLabel.equals(k.length > 1 ? k[1] : null))) {
                    drop.add(e.getKey());
                }
            }
            for (String k : drop) {
                schemas.remove(k);
            }
            catalog.apply(m, clock);
            return;
        }
        if (catalog.apply(m, clock)) {
            schemas.put(m.id + SEP + m.versionLabel, m.payload);
        }
        List<String> expired = new ArrayList<>();
        for (Map.Entry<String, String> e : schemas.entries()) {
            String[] k = e.getKey().split(SEP, 2);
            if (k.length == 2 && !catalog.has(k[0] + "_" + k[1])) {
                expired.add(e.getKey());
            }
        }
        for (String k : expired) {
            schemas.remove(k);
        }
    }

    private void flushColdBuffer(Consumer<KeyedRecord> dirty, Collector<ValidatedEvent> out, boolean force) {
        List<RawEvent> pending = coldBuffer;
        coldBuffer = new ArrayList<>();
        for (RawEvent e : pending) {
            handle(e, dirty, out, !force);
        }
    }

    private void handle(RawEvent raw, Consumer<KeyedRecord> dirty, Collector<ValidatedEvent> out, boolean allowBuffer) {
        JsonNode node;
        try {
            node = MAPPER.readTree(raw.value);
        } catch (Exception e) {
            ObjectNode err = MAPPER.createObjectNode();
            err.put("original_payload", raw.value);
            err.put("error_reason", "JSON Parse Exception: " + e.getMessage());
            emitDirty(dirty, null, err);
            return;
        }
        if (node == null || !node.isObject()) {
            ObjectNode err = MAPPER.createObjectNode();
            err.put("original_payload", raw.value);
            err.put("error_reason", "Event is not a JSON object");
            emitDirty(dirty, null, err);
            return;
        }
        ObjectNode ev = (ObjectNode) node;
        if (!ev.hasNonNull("source_system") || !ev.hasNonNull("event_type") || !ev.hasNonNull("schema_version")) {
            ev.put("error_reason", "Missing source_system or event_type or schema_version for schema mapping");
            emitDirty(dirty, ev, ev);
            return;
        }
        String key = SchemaCatalog.schemaKey(ev.get("source_system").asText(), ev.get("event_type").asText(),
                ev.get("schema_version").asText());
        String reason = catalog.validate(key, ev);
        if (reason != null && reason.startsWith(SCHEMA_NOT_FOUND) && allowBuffer
                && coldBuffer.size() < config.coldStartBufferMax) {
            coldBuffer.add(raw);
            bufferedCounter.inc();
            return;
        }
        if (reason != null) {
            ev.put("error_reason", reason);
            emitDirty(dirty, ev, ev);
            return;
        }
        if (!ev.hasNonNull("entity_id") || ev.get("entity_id").asText().isEmpty()) {
            ev.put("error_reason", "Missing entity_id");
            emitDirty(dirty, ev, ev);
            return;
        }
        long eventTime = ev.hasNonNull("event_time") ? KafkaIo.extractEventTime(raw.value) : Long.MIN_VALUE;
        if (eventTime == Long.MIN_VALUE) {
            ev.put("error_reason", "Missing or unparseable ISO-8601 event_time");
            emitDirty(dirty, ev, ev);
            return;
        }
        String rawJson = config.includePayload ? raw.value : null;
        out.collect(catalog.project(ev, eventTime, raw.kafkaTimestamp, config.projectionEnabled, rawJson));
        validCounter.inc();
    }

    private void emitDirty(Consumer<KeyedRecord> dirty, JsonNode event, ObjectNode body) {
        String key = event != null && event.hasNonNull("entity_id") ? event.get("entity_id").asText() : null;
        dirty.accept(new KeyedRecord(key, body.toString()));
        dirtyCounter.inc();
    }
}
