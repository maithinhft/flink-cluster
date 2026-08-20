package generator.configs;

import java.sql.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.postgresql.util.PGobject;

public class ConfigGenerator {

    // ============================================================
    // PostgreSQL configuration
    // ============================================================

    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5433/realtime_core";
    private static final String DEFAULT_DB_USER = "postgres";
    private static final String DEFAULT_DB_PASSWORD = "postgres";

    // ============================================================
    // Generator configuration
    // ============================================================

    private static final int DEFAULT_NUM_RULES = 100_000;
    private static final int DEFAULT_BATCH_SIZE = 5_000;

    private static final Random RANDOM = new Random(42);

    // ============================================================
    // Dictionaries matching EventGenerator concrete schema
    // ============================================================

    private static final String[] ENTITY_TYPES = {
            "customer", "device", "account"
    };

    // Fields for exact matches (EQ, NEQ, IN)
    private static final String[] TAG_FIELDS = {
            "product_category", "country_code", "browser_name", "device_type", 
            "user_segment", "payment_method", "order_status", "account_status", "payment_gateway"
    };

    private static final String[][] TAG_VALUES = {
            {"electronics", "fashion", "food", "books"},
            {"VN", "US", "JP", "KR", "SG", "TH"},
            {"Chrome", "Firefox", "Safari", "Edge"},
            {"mobile", "tablet", "desktop"},
            {"new", "returning", "vip", "churn_risk"},
            {"credit_card", "wallet", "bank_transfer"},
            {"created", "paid", "shipped"},
            {"active", "suspended"},
            {"stripe", "paypal", "vnpay"}
    };

    // Fields for numeric comparison (GT, LT, BETWEEN)
    private static final String[] NUM_FIELDS = {
            "total_amount", "user_age", "loyalty_points", "time_on_page_ms", 
            "fraud_score", "battery_level_percent", "quantity"
    };

    // Comparison operators
    private static final String[] NUMERIC_OPS = {"GT", "GTE", "LT", "LTE"};

    // Rule name prefixes for readability
    private static final String[] RULE_PREFIXES = {
            "high_value_segment", "active_user_filter", "churn_risk_detect",
            "vip_upgrade_check", "engagement_score_rule", "purchase_frequency",
            "retention_campaign", "cross_sell_trigger", "loyalty_tier_eval",
            "dormant_reactivation", "geo_target_segment", "device_pref_rule"
    };

    // ============================================================
    // SQL — rules only
    // ============================================================

    private static final String INSERT_RULE = """
            INSERT INTO rule_definitions (
                rule_id,
                name,
                entity_type,
                rule_json,
                priority,
                cooldown_seconds,
                tags,
                version,
                enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    // ============================================================
    // Main
    // ============================================================

    public static void main(String[] args) throws Exception {
        String dbUrl = DEFAULT_DB_URL;
        String dbUser = DEFAULT_DB_USER;
        String dbPassword = DEFAULT_DB_PASSWORD;
        int numRules = DEFAULT_NUM_RULES;
        int batchSize = DEFAULT_BATCH_SIZE;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--num-rules": numRules = Integer.parseInt(args[++i]); break;
                case "--batch-size": batchSize = Integer.parseInt(args[++i]); break;
                case "--db-url": dbUrl = args[++i]; break;
                case "--db-user": dbUser = args[++i]; break;
                case "--db-password": dbPassword = args[++i]; break;
            }
        }

        System.out.println("======================================================");
        System.out.println("PostgreSQL Rule Definition Generator (Concrete Fields)");
        System.out.println("======================================================");
        System.out.printf("Rules        : %,d%n", numRules);

        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            connection.setAutoCommit(false);
            long start = System.nanoTime();

            generateRules(connection, numRules, batchSize);

            connection.commit();
            double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;

            System.out.println("======================================================");
            System.out.println("Generation completed");
            System.out.printf("Elapsed      : %.2f s%n", elapsed);
            System.out.printf("Throughput   : %,.0f rules/s%n", numRules / elapsed);
        }
    }

    // ============================================================
    // Rule generation
    // ============================================================

    private static void generateRules(Connection connection, int count, int batchSize) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_RULE)) {
            for (int i = 0; i < count; i++) {
                UUID ruleId = UUID.randomUUID();
                String name = RULE_PREFIXES[RANDOM.nextInt(RULE_PREFIXES.length)] + "_" + (i + 1);
                String entityType = randomElement(ENTITY_TYPES);
                String ruleJson = generateConditionTree(2 + RANDOM.nextInt(3)); // depth 2-4
                int priority = RANDOM.nextInt(100);
                long cooldownSeconds = randomElement(new Long[]{0L, 60L, 300L, 900L, 3600L});
                
                int numTags = 1 + RANDOM.nextInt(4);
                String[] tags = new String[numTags];
                for (int t = 0; t < numTags; t++) {
                    int tagIdx = RANDOM.nextInt(TAG_VALUES.length);
                    tags[t] = TAG_VALUES[tagIdx][RANDOM.nextInt(TAG_VALUES[tagIdx].length)];
                }
                
                long version = 1 + RANDOM.nextInt(9);
                boolean enabled = RANDOM.nextDouble() < 0.95;

                ps.setObject(1, ruleId);
                ps.setString(2, name);
                ps.setString(3, entityType);

                PGobject jsonObject = new PGobject();
                jsonObject.setType("jsonb");
                jsonObject.setValue(ruleJson);
                ps.setObject(4, jsonObject);

                ps.setInt(5, priority);
                ps.setLong(6, cooldownSeconds);
                ps.setArray(7, connection.createArrayOf("text", tags));
                ps.setLong(8, version);
                ps.setBoolean(9, enabled);

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

    // ============================================================
    // Condition tree generation
    // ============================================================

    private static String generateConditionTree(int maxDepth) {
        return generateNode(0, maxDepth);
    }

    private static String generateNode(int currentDepth, int maxDepth) {
        if (currentDepth >= maxDepth || (currentDepth > 0 && RANDOM.nextDouble() < 0.4)) {
            return generateLeafClause();
        }

        String operator;
        double r = RANDOM.nextDouble();
        if (r < 0.45) operator = "AND";
        else if (r < 0.90) operator = "OR";
        else operator = "NOT";

        if ("NOT".equals(operator)) {
            return """
                    {
                      "operator": "NOT",
                      "children": [%s]
                    }""".formatted(generateNode(currentDepth + 1, maxDepth));
        }

        int numChildren = 2 + RANDOM.nextInt(3);
        StringBuilder sb = new StringBuilder();
        sb.append("{\n  \"operator\": \"").append(operator).append("\",\n  \"children\": [\n");

        for (int i = 0; i < numChildren; i++) {
            if (i > 0) sb.append(",\n");
            sb.append("    ").append(generateNode(currentDepth + 1, maxDepth));
        }
        sb.append("\n  ]\n}");
        return sb.toString();
    }

    private static String generateLeafClause() {
        if (RANDOM.nextBoolean()) return generateAggregationClause();
        return generateRawFieldClause();
    }

    private static String generateAggregationClause() {
        return """
                {
                  "type": "AGGREGATION",
                  "aggregation_id": "%s",
                  "operator": "%s",
                  "value": %d
                }""".formatted(UUID.randomUUID().toString(), randomElement(NUMERIC_OPS), RANDOM.nextInt(1, 10_000));
    }

    private static String generateRawFieldClause() {
        double r = RANDOM.nextDouble();

        if (r < 0.40) {
            // String exact match (Tag)
            int tagIdx = RANDOM.nextInt(TAG_FIELDS.length);
            String field = TAG_FIELDS[tagIdx];
            String[] pool = TAG_VALUES[tagIdx];

            double opRoll = RANDOM.nextDouble();
            if (opRoll < 0.5) {
                return """
                        {
                          "type": "RAW_FIELD",
                          "field": "%s",
                          "operator": "EQ",
                          "value": "%s"
                        }""".formatted(field, randomElement(pool));
            } else if (opRoll < 0.8) {
                int count = 2 + RANDOM.nextInt(3);
                Set<String> vals = new LinkedHashSet<>();
                while (vals.size() < count && vals.size() < pool.length) vals.add(randomElement(pool));
                return """
                        {
                          "type": "RAW_FIELD",
                          "field": "%s",
                          "operator": "IN",
                          "value": ["%s"]
                        }""".formatted(field, String.join("\", \"", vals));
            } else {
                return """
                        {
                          "type": "RAW_FIELD",
                          "field": "%s",
                          "operator": "NEQ",
                          "value": "%s"
                        }""".formatted(field, randomElement(pool));
            }

        } else if (r < 0.75) {
            // Numeric field
            String field = randomElement(NUM_FIELDS);
            if (RANDOM.nextDouble() < 0.2) {
                int lo = RANDOM.nextInt(1, 5000);
                int hi = lo + RANDOM.nextInt(1, 5000);
                return """
                        {
                          "type": "RAW_FIELD",
                          "field": "%s",
                          "operator": "BETWEEN",
                          "value": [%d, %d]
                        }""".formatted(field, lo, hi);
            } else {
                return """
                        {
                          "type": "RAW_FIELD",
                          "field": "%s",
                          "operator": "%s",
                          "value": %d
                        }""".formatted(field, randomElement(NUMERIC_OPS), RANDOM.nextInt(1, 10_000));
            }

        } else if (r < 0.90) {
            // Boolean field
            String field = randomElement(new String[]{"is_first_open", "vpn_detected", "bounce", "opt_in_email", "is_gift"});
            return """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "EQ",
                      "value": %s
                    }""".formatted(field, RANDOM.nextBoolean());

        } else {
            // String CONTAINS
            String field = randomElement(new String[]{"page_url", "user_email", "product_name"});
            return """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "CONTAINS",
                      "value": "example"
                    }""".formatted(field);
        }
    }

    @SafeVarargs
    private static <T> T randomElement(T... values) {
        return values[RANDOM.nextInt(values.length)];
    }
}