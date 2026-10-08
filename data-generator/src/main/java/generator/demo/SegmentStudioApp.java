package generator.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import generator.common.ClientConfig;
import generator.events.EventConfig;
import generator.events.EventFactory;
import generator.rules.RuleConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * "Segment Studio" demo (output requirement 4): declare segmentation criteria, fire sample events for a demo customer
 * and watch the realtime engine's alerts arrive within seconds — no job restart.
 *
 * <ul>
 *   <li>Rules are written as a new version into {@code rule_definitions}; Debezium CDC carries them to Flink.</li>
 *   <li>Events are produced to the same topics/clusters as the generator (CRM via Kerberos, others via PLAIN).</li>
 *   <li>Alerts are read live from the {@code result} topic by a background consumer.</li>
 * </ul>
 * Run: {@code mvn exec:java -Dexec.mainClass=generator.demo.SegmentStudioApp -Dexec.args="--port 8090"} then open
 * http://localhost:8090
 */
public class SegmentStudioApp {

    private static final ObjectMapper M = new ObjectMapper();
    private static final int MAX_ALERTS = 500;

    private final EventConfig cfg;
    private final String dbUrl;
    private final Deque<ObjectNode> alerts = new ArrayDeque<>();
    private final KafkaProducer<String, byte[]> plainProducer;
    private final boolean useGssapiForCrm;
    private KafkaProducer<String, byte[]> gssapiProducer;
    private final Random random = new Random();

    SegmentStudioApp(EventConfig cfg, String dbUrl) {
        this.cfg = cfg;
        this.dbUrl = dbUrl;
        boolean dual = "dual".equalsIgnoreCase(cfg.cluster) || "multi".equalsIgnoreCase(cfg.cluster);
        this.plainProducer = new KafkaProducer<>(cfg.getPlainProducerProperties());
        this.useGssapiForCrm = dual || "gssapi".equalsIgnoreCase(cfg.cluster);
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(ClientConfig.arg(args, "--port", "8090"));
        EventConfig cfg = ClientConfig.eventConfig(args);
        SegmentStudioApp app = new SegmentStudioApp(cfg, ClientConfig.arg(args, "--db-url", null));
        Thread consumer = new Thread(() -> app.consumeAlerts(ClientConfig.arg(args, "--result-topic", "result")), "alert-consumer");
        consumer.setDaemon(true);
        consumer.start();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.createContext("/", app::handle);
        server.start();
        System.out.printf("Segment Studio running on http://localhost:%d%n", port);
    }

    private void consumeAlerts(String topic) {
        Properties p = ClientConfig.consumer(cfg, false, "segment-studio-" + UUID.randomUUID());
        p.put("auto.offset.reset", "latest");
        p.put("enable.auto.commit", "true");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(300))) {
                    try {
                        ObjectNode a = (ObjectNode) M.readTree(r.value());
                        a.put("received_at_ms", System.currentTimeMillis());
                        synchronized (alerts) {
                            alerts.addFirst(a);
                            while (alerts.size() > MAX_ALERTS) {
                                alerts.removeLast();
                            }
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Alert consumer stopped: " + e.getMessage());
        }
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        String method = ex.getRequestMethod();
        try {
            if ("GET".equals(method) && ("/".equals(path) || "/index.html".equals(path))) {
                try (InputStream in = getClass().getResourceAsStream("/demo/index.html")) {
                    send(ex, 200, "text/html; charset=utf-8", in.readAllBytes());
                }
            } else if ("GET".equals(method) && "/api/meta".equals(path)) {
                json(ex, 200, meta());
            } else if ("GET".equals(method) && "/api/rules".equals(path)) {
                json(ex, 200, listRules());
            } else if ("POST".equals(method) && "/api/rules".equals(path)) {
                json(ex, 200, saveRule(body(ex), true));
            } else if ("POST".equals(method) && "/api/rules/disable".equals(path)) {
                json(ex, 200, saveRule(body(ex), false));
            } else if ("POST".equals(method) && "/api/events".equals(path)) {
                json(ex, 200, sendEvent(body(ex)));
            } else if ("GET".equals(method) && "/api/alerts".equals(path)) {
                json(ex, 200, recentAlerts(query(ex, "since")));
            } else {
                json(ex, 404, M.createObjectNode().put("error", "not found"));
            }
        } catch (IllegalArgumentException e) {
            json(ex, 400, M.createObjectNode().put("error", e.getMessage()));
        } catch (Exception e) {
            json(ex, 500, M.createObjectNode().put("error", e.toString()));
        }
    }

    private ObjectNode meta() {
        ObjectNode m = M.createObjectNode();
        ArrayNode types = m.putArray("event_types");
        for (String t : RuleConfig.EVENT_TYPES) {
            types.add(t);
        }
        ArrayNode tags = m.putArray("tag_fields");
        for (String t : RuleConfig.TAG_FIELDS) {
            tags.add(t);
        }
        ArrayNode nums = m.putArray("numeric_fields");
        for (String t : RuleConfig.NUM_FIELDS) {
            nums.add(t);
        }
        return m;
    }

    private ArrayNode listRules() throws Exception {
        ArrayNode out = M.createArrayNode();
        String sql = "SELECT * FROM (SELECT DISTINCT ON (rule_id) rule_id::text AS rule_id, name, rule_json::text AS rule_json, "
                + "cooldown_seconds, version, enabled, updated_at FROM rule_definitions ORDER BY rule_id, version DESC) r "
                + "ORDER BY updated_at DESC LIMIT 100";
        try (Connection c = ClientConfig.db(dbUrl); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ObjectNode r = out.addObject();
                r.put("rule_id", rs.getString("rule_id"));
                r.put("name", rs.getString("name"));
                r.set("rule_json", M.readTree(rs.getString("rule_json")));
                r.put("cooldown_seconds", rs.getLong("cooldown_seconds"));
                r.put("version", rs.getLong("version"));
                r.put("enabled", rs.getBoolean("enabled"));
                r.put("updated_at_ms", rs.getTimestamp("updated_at").getTime());
            }
        }
        return out;
    }

    private ObjectNode saveRule(JsonNode req, boolean enabled) throws Exception {
        String ruleId = req.path("rule_id").asText("");
        if (req.has("rule_json") && !req.get("rule_json").has("trigger_events") && !req.get("rule_json").has("trigger_event")) {
            throw new IllegalArgumentException("rule_json needs trigger_events (rules without triggers never fire)");
        }
        try (Connection c = ClientConfig.db(dbUrl)) {
            c.setAutoCommit(false);
            String name = req.path("name").asText("");
            String ruleJson = req.has("rule_json") ? req.get("rule_json").toString() : null;
            long cooldown = req.path("cooldown_seconds").asLong(0);
            long version = 1;
            if (!ruleId.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement("SELECT name, rule_json::text, cooldown_seconds, version "
                        + "FROM rule_definitions WHERE rule_id = ?::uuid ORDER BY version DESC LIMIT 1 FOR UPDATE")) {
                    ps.setString(1, ruleId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            version = rs.getLong(4) + 1;
                            if (name.isEmpty()) name = rs.getString(1);
                            if (ruleJson == null) ruleJson = rs.getString(2);
                            if (!req.has("cooldown_seconds")) cooldown = rs.getLong(3);
                        } else if (!enabled) {
                            throw new IllegalArgumentException("Unknown rule_id " + ruleId);
                        }
                    }
                }
            } else {
                ruleId = UUID.randomUUID().toString();
            }
            if (ruleJson == null) {
                throw new IllegalArgumentException("rule_json is required");
            }
            Timestamp now = Timestamp.from(Instant.now());
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO rule_definitions (rule_id, name, rule_json, "
                    + "cooldown_seconds, version, enabled, created_at, updated_at, user_id) VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?, 'segment-studio')")) {
                PGobject j = new PGobject();
                j.setType("jsonb");
                j.setValue(ruleJson);
                ps.setString(1, ruleId);
                ps.setString(2, name.isEmpty() ? "segment-" + ruleId.substring(0, 8) : name);
                ps.setObject(3, j);
                ps.setLong(4, cooldown);
                ps.setLong(5, version);
                ps.setBoolean(6, enabled);
                ps.setTimestamp(7, now);
                ps.setTimestamp(8, now);
                ps.executeUpdate();
            }
            c.commit();
            return M.createObjectNode().put("rule_id", ruleId).put("version", version).put("enabled", enabled)
                    .put("saved_at_ms", now.getTime());
        }
    }

    private ObjectNode sendEvent(JsonNode req) throws Exception {
        String entity = req.path("entity_id").asText("");
        String type = req.path("event_type").asText("");
        if (entity.isEmpty() || type.isEmpty()) {
            throw new IllegalArgumentException("entity_id and event_type are required");
        }
        Map<String, Object> event = EventFactory.generateEventOfType(entity, type, Instant.now(), random);
        JsonNode fields = req.get("fields");
        if (fields != null && fields.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = fields.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> f = it.next();
                event.put(f.getKey(), M.treeToValue(f.getValue(), Object.class));
            }
        }
        String source = (String) event.get("source_system");
        String topic = "crm".equals(source) ? cfg.crmTopic : ("payment".equals(source) ? cfg.paymentTopic : cfg.ecommerceTopic);
        KafkaProducer<String, byte[]> producer = "crm".equals(source) && useGssapiForCrm ? gssapi() : plainProducer;
        producer.send(new ProducerRecord<>(topic, entity, M.writeValueAsBytes(event))).get();
        ObjectNode out = M.valueToTree(event);
        out.put("_topic", topic);
        out.put("_sent_at_ms", System.currentTimeMillis());
        return out;
    }

    private synchronized KafkaProducer<String, byte[]> gssapi() {
        if (gssapiProducer == null) {
            gssapiProducer = new KafkaProducer<>(cfg.getGssapiProducerProperties());
        }
        return gssapiProducer;
    }

    private ArrayNode recentAlerts(String since) {
        long sinceMs = since == null ? 0L : Long.parseLong(since);
        ArrayNode out = M.createArrayNode();
        synchronized (alerts) {
            for (ObjectNode a : alerts) {
                if (a.path("received_at_ms").asLong() > sinceMs) {
                    out.add(a);
                }
            }
        }
        return out;
    }

    private static JsonNode body(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            byte[] b = in.readAllBytes();
            return b.length == 0 ? M.createObjectNode() : M.readTree(b);
        }
    }

    private static String query(HttpExchange ex, String key) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return null;
        for (String part : q.split("&")) {
            int eq = part.indexOf('=');
            if (eq > 0 && part.substring(0, eq).equals(key)) return part.substring(eq + 1);
        }
        return null;
    }

    private static void json(HttpExchange ex, int status, JsonNode body) throws IOException {
        send(ex, status, "application/json", M.writeValueAsBytes(body));
    }

    private static void send(HttpExchange ex, int status, String type, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", type);
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }
}
