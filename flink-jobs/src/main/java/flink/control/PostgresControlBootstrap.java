package flink.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.model.ControlMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Cold-start bootstrap: reads the current schemas and the latest version of every rule from PostgreSQL so
 * that a freshly started job can validate and evaluate events before CDC has replayed the control topics.
 * CDC records that arrive later are deduplicated by version, so double delivery is harmless.
 */
public final class PostgresControlBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresControlBootstrap.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String RULES_SQL =
            "SELECT DISTINCT ON (rule_id) rule_id::text AS rule_id, name, rule_json::text AS rule_json, "
                    + "cooldown_seconds, version, enabled, created_at, updated_at "
                    + "FROM rule_definitions ORDER BY rule_id, version DESC";

    private static final String SCHEMAS_SQL =
            "SELECT schema_id, schema_payload::text AS schema_payload FROM schema_definitions";

    private PostgresControlBootstrap() {
    }

    public static List<ControlMessage> load(String url, String user, String password, boolean rules, boolean schemas) {
        if (url == null || url.isEmpty()) {
            return Collections.emptyList();
        }
        List<ControlMessage> out = new ArrayList<>();
        long start = System.currentTimeMillis();
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LOG.warn("PostgreSQL driver missing, skipping bootstrap");
            return out;
        }
        try (Connection c = DriverManager.getConnection(url, user, password);
             Statement st = c.createStatement()) {
            st.setQueryTimeout(30);
            if (schemas) {
                try (ResultSet rs = st.executeQuery(SCHEMAS_SQL)) {
                    while (rs.next()) {
                        ObjectNode row = MAPPER.createObjectNode();
                        row.put("schema_id", rs.getString("schema_id"));
                        row.put("schema_payload", rs.getString("schema_payload"));
                        row.put("__table", "schema_definitions");
                        ControlMessage m = ControlMessageParser.parse(row.toString());
                        if (m != null) {
                            out.add(m);
                        }
                    }
                }
            }
            if (rules) {
                try (ResultSet rs = st.executeQuery(RULES_SQL)) {
                    while (rs.next()) {
                        ObjectNode row = ControlMessageParser.ruleRow(
                                rs.getString("rule_id"), rs.getString("name"), rs.getString("rule_json"),
                                rs.getLong("cooldown_seconds"), rs.getLong("version"), rs.getBoolean("enabled"),
                                millis(rs.getTimestamp("created_at")), millis(rs.getTimestamp("updated_at")));
                        ControlMessage m = ControlMessageParser.parse(row.toString());
                        if (m != null) {
                            out.add(m);
                        }
                    }
                }
            }
            LOG.info("Bootstrapped {} control records from PostgreSQL in {} ms", out.size(),
                    System.currentTimeMillis() - start);
        } catch (Exception e) {
            LOG.warn("Control bootstrap from {} failed ({}); relying on CDC replay only", url, e.getMessage());
        }
        return out;
    }

    private static long millis(Timestamp t) {
        return t == null ? 0L : t.getTime();
    }
}
