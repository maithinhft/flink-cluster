package generator.configs;

import java.sql.*;
import java.util.*;
import org.postgresql.util.PGobject;

public class ConfigGenerator {

    // ============================================================
    // PostgreSQL configuration
    // ============================================================

    private static final String DEFAULT_DB_URL = "jdbc:postgresql://34.55.131.15:5433/realtime_core";
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

    // Fields for exact matches (EQ, NEQ, IN)
    private static final String[] TAG_FIELDS = {
            "product_category", "payment_method", "order_status",
            "account_status", "subscription_tier", "support_ticket_status", "transaction_status"
    };

    private static final String[][] TAG_VALUES = {
            { "electronics", "fashion", "food", "books" },
            { "credit_card", "wallet", "bank_transfer" },
            { "created", "paid", "shipped", "completed", "cancelled" },
            { "active", "suspended", "closed" },
            { "free", "basic", "premium" },
            { "open", "in_progress", "resolved" },
            { "pending", "success", "failed" }
    };

    // Fields for numeric comparison (GT, LT, BETWEEN)
    private static final String[] NUM_FIELDS = {
            "total_amount", "loyalty_points", "quantity",
            "satisfaction_score", "unit_price", "discount_amount", "tax_amount"
    };

    // Comparison operators
    private static final String[] NUMERIC_OPS = { "GT", "GTE", "LT", "LTE" };

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
                rule_json,
                priority,
                cooldown_seconds,
                version,
                enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
                case "--num-rules":
                    numRules = Integer.parseInt(args[++i]);
                    break;
                case "--batch-size":
                    batchSize = Integer.parseInt(args[++i]);
                    break;
                case "--db-url":
                    dbUrl = args[++i];
                    break;
                case "--db-user":
                    dbUser = args[++i];
                    break;
                case "--db-password":
                    dbPassword = args[++i];
                    break;
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
                String ruleJson = generateConditionTree(2 + RANDOM.nextInt(3)); // depth 2-4
                int priority = RANDOM.nextInt(100);
                long cooldownSeconds = randomElement(new Long[] { 0L, 60L, 300L, 900L, 3600L });
                long version = 1 + RANDOM.nextInt(9);
                boolean enabled = RANDOM.nextDouble() < 0.95;

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
        if (r < 0.45)
            operator = "AND";
        else if (r < 0.90)
            operator = "OR";
        else
            operator = "NOT";

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
            if (i > 0)
                sb.append(",\n");
            sb.append("    ").append(generateNode(currentDepth + 1, maxDepth));
        }
        sb.append("\n  ]\n}");
        return sb.toString();
    }

    private static String generateLeafClause() {
        if (RANDOM.nextBoolean())
            return generateAggregationClause();
        return generateRawFieldClause();
    }

    private static String generateAggregationClause() {
        String function = randomElement(new String[] { "SUM", "COUNT", "AVG", "MAX", "MIN" });
        String field = randomElement(NUM_FIELDS); // Simplification: just use numeric fields for all functions in mock
                                                  // data
        int size = randomElement(new Integer[] { 300, 3600, 21600, 86400 });
        int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
        String windowType = size == slide ? "tumbling" : "sliding";

        return """
                {
                  "type": "AGGREGATION",
                  "field": "%s",
                  "function": "%s",
                  "window": {
                    "type": "%s",
                    "size_seconds": %d,
                    "slide_seconds": %d
                  },
                  "operator": "%s",
                  "value": %d
                }""".formatted(field, function, windowType, size, slide, randomElement(NUMERIC_OPS),
                RANDOM.nextInt(1, 10_000));
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
                while (vals.size() < count && vals.size() < pool.length)
                    vals.add(randomElement(pool));
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
            String field = randomElement(new String[] { "opt_in_email", "opt_in_sms", "opt_in_push", "is_3ds_verified",
                    "billing_zip_match" });
            return """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "EQ",
                      "value": %s
                    }""".formatted(field, RANDOM.nextBoolean());

        } else {
            // String CONTAINS
            String field = randomElement(
                    new String[] { "user_email", "user_first_name", "user_last_name", "product_name" });
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