package generator.rules;

import generator.common.RandomUtils;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RuleDao {

    private static final String INSERT_RULE = """
            INSERT INTO rule_definitions (
                rule_id,
                name,
                rule_json,
                cooldown_seconds,
                version,
                enabled,
                created_at,
                updated_at,
                user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String[] USER_IDS = {
            "admin-001", "admin-002", "ops-001", "ops-002",
            "analyst-001", "analyst-002", "analyst-003"
    };

    static class RuleState {
        UUID ruleId;
        String name;
        long version;
        Instant nextUpdateTime;
        Instant createdAt;
    }

    public static void generateAndInsertRules(Connection connection, int count, int batchSize, boolean continuous) throws SQLException {
        List<RuleState> activeRules = new ArrayList<>();
        
        try (PreparedStatement ps = connection.prepareStatement(INSERT_RULE)) {
            for (int i = 0; continuous || i < count; i++) {
                RuleState stateToUpdate = null;
                Instant now = Instant.now();

                if (!activeRules.isEmpty() && RandomUtils.RANDOM.nextDouble() < 0.8) {
                    RuleState candidate = activeRules.get(RandomUtils.RANDOM.nextInt(activeRules.size()));
                    if (!continuous || candidate.nextUpdateTime.isBefore(now)) {
                        stateToUpdate = candidate;
                    }
                }

                if (stateToUpdate == null) {
                    stateToUpdate = new RuleState();
                    stateToUpdate.ruleId = UUID.randomUUID();
                    stateToUpdate.name = RuleConfig.RULE_PREFIXES[RandomUtils.RANDOM.nextInt(RuleConfig.RULE_PREFIXES.length)] + "_" + (activeRules.size() + 1);
                    stateToUpdate.version = 1;
                    stateToUpdate.createdAt = continuous ? now : now.minus(RandomUtils.RANDOM.nextInt(90), ChronoUnit.DAYS);
                    stateToUpdate.nextUpdateTime = now; // Ready to be updated immediately
                    if (activeRules.size() < 50_000) {
                        activeRules.add(stateToUpdate);
                    }
                } else {
                    stateToUpdate.version++;
                    if (!continuous) {
                        stateToUpdate.createdAt = stateToUpdate.createdAt.plus(RandomUtils.RANDOM.nextInt(1440), ChronoUnit.MINUTES);
                    }
                }

                // Random delay until next possible update (2 to 30 seconds real time)
                stateToUpdate.nextUpdateTime = now.plusSeconds(RandomUtils.randomInt(2, 30));

                String ruleJson = RuleFactory.generateTopLevelRuleJson(2 + RandomUtils.RANDOM.nextInt(3)); // depth 2-4
                long cooldownSeconds = RandomUtils.randomLong(0L, 60L, 300L, 900L, 3600L);
                boolean enabled = RandomUtils.RANDOM.nextDouble() < 0.95;

                Timestamp createdAt = Timestamp.from(continuous && stateToUpdate.version > 1 ? stateToUpdate.createdAt : stateToUpdate.createdAt); 
                Timestamp updatedAt = continuous ? Timestamp.from(now) : Timestamp.from(stateToUpdate.createdAt);
                String userId = USER_IDS[RandomUtils.RANDOM.nextInt(USER_IDS.length)];

                ps.setObject(1, stateToUpdate.ruleId);
                ps.setString(2, stateToUpdate.name);

                PGobject jsonObject = new PGobject();
                jsonObject.setType("jsonb");
                jsonObject.setValue(ruleJson);
                ps.setObject(3, jsonObject);

                ps.setLong(4, cooldownSeconds);
                ps.setLong(5, stateToUpdate.version);
                ps.setBoolean(6, enabled);
                ps.setTimestamp(7, createdAt);
                ps.setTimestamp(8, updatedAt);
                ps.setString(9, userId);

                ps.addBatch();

                if ((i + 1) % batchSize == 0) {
                    ps.executeBatch();
                    connection.commit();
                    System.out.printf("Rules: %,d%n", i + 1);
                    if (continuous) {
                        try { Thread.sleep(100); } catch (InterruptedException e) {}
                    }
                }
            }
            if (!continuous) {
                ps.executeBatch();
            }
        }
    }
}

