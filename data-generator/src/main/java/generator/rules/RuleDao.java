package generator.rules;

import generator.common.RandomUtils;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

public class RuleDao {

    private static final String INSERT_RULE = """
            INSERT INTO rule_definitions (
                rule_id,
                name,
                rule_json,
                priority,
                cooldown_seconds,
                version,
                enabled,
                created_at,
                updated_at,
                user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String[] USER_IDS = {
            "admin-001", "admin-002", "ops-001", "ops-002",
            "analyst-001", "analyst-002", "analyst-003"
    };

    public static void generateAndInsertRules(Connection connection, int count, int batchSize) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_RULE)) {
            for (int i = 0; i < count; i++) {
                UUID ruleId = UUID.randomUUID();
                String name = RuleConfig.RULE_PREFIXES[RandomUtils.RANDOM.nextInt(RuleConfig.RULE_PREFIXES.length)] + "_" + (i + 1);
                String ruleJson = RuleFactory.generateConditionTree(2 + RandomUtils.RANDOM.nextInt(3)); // depth 2-4
                int priority = RandomUtils.RANDOM.nextInt(100);
                long cooldownSeconds = RandomUtils.randomLong(0L, 60L, 300L, 900L, 3600L);
                long version = 1 + RandomUtils.RANDOM.nextInt(9);
                boolean enabled = RandomUtils.RANDOM.nextDouble() < 0.95;

                Instant createdInstant = Instant.now().minus(RandomUtils.RANDOM.nextInt(90), ChronoUnit.DAYS);
                Instant updatedInstant = createdInstant.plus(RandomUtils.RANDOM.nextInt(30), ChronoUnit.DAYS);
                Timestamp createdAt = Timestamp.from(createdInstant);
                Timestamp updatedAt = Timestamp.from(updatedInstant);
                String userId = USER_IDS[RandomUtils.RANDOM.nextInt(USER_IDS.length)];

                ps.setObject(1, ruleId);
                ps.setString(2, name);

                PGobject jsonObject = new PGobject();
                jsonObject.setType("jsonb");
                jsonObject.setValue(ruleJson);
                ps.setObject(3, jsonObject);

                ps.setInt(4, priority);
                ps.setLong(5, cooldownSeconds);
                ps.setLong(6, version);
                ps.setBoolean(7, enabled);
                ps.setTimestamp(8, createdAt);
                ps.setTimestamp(9, updatedAt);
                ps.setString(10, userId);

                ps.addBatch();

                if ((i + 1) % batchSize == 0) {
                    ps.executeBatch();
                    connection.commit();
                    System.out.printf("Rules: %,d / %,d%n", i + 1, count);
                }
            }
            ps.executeBatch();
        }
    }
}

