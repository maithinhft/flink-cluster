package generator.rules;

import generator.common.RandomUtils;
import java.util.LinkedHashSet;
import java.util.Set;

public class RuleFactory {

    public static String generateConditionTree(int maxDepth) {
        return generateNode(0, maxDepth);
    }

    private static String generateNode(int currentDepth, int maxDepth) {
        if (currentDepth >= maxDepth || (currentDepth > 0 && RandomUtils.RANDOM.nextDouble() < 0.4)) {
            return generateLeafClause();
        }

        String operator;
        double r = RandomUtils.RANDOM.nextDouble();
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

        int numChildren = 2 + RandomUtils.RANDOM.nextInt(3);
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
        double r = RandomUtils.RANDOM.nextDouble();
        if (r < 0.25)
            return generateAggregationClause();
        else if (r < 0.50)
            return generateEventCountAggregationClause();
        else if (r < 0.75)
            return generateSourceSystemCountAggregationClause();
        return generateRawFieldClause();
    }

    private static String generateSourceSystemCountAggregationClause() {
        String sourceSystem = RandomUtils.randomElement("ecommerce", "crm", "payment");
        int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
        int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
        String windowType = size == slide ? "tumbling" : "sliding";

        String filterJson = "null";
        if (RandomUtils.RANDOM.nextBoolean()) {
            int tagIdx = RandomUtils.RANDOM.nextInt(RuleConfig.TAG_FIELDS.length);
            String tagField = RuleConfig.TAG_FIELDS[tagIdx];
            String tagValue = RandomUtils.randomElement(RuleConfig.TAG_VALUES[tagIdx]);
            filterJson = """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "EQ",
                      "value": "%s"
                    }""".formatted(tagField, tagValue);
        }

        return """
                {
                  "type": "AGGREGATION",
                  "field": "event_id",
                  "function": "COUNT",
                  "source_system": "%s",
                  "window": {
                    "type": "%s",
                    "size_seconds": %d,
                    "slide_seconds": %d
                  },
                  "filter": %s,
                  "operator": "%s",
                  "value": %d
                }""".formatted(sourceSystem, windowType, size, slide, filterJson, RandomUtils.randomElement(RuleConfig.NUMERIC_OPS),
                RandomUtils.RANDOM.nextInt(50) + 1);
    }

    private static String generateEventCountAggregationClause() {
        String eventType = RandomUtils.randomElement(RuleConfig.EVENT_TYPES);
        int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
        int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
        String windowType = size == slide ? "tumbling" : "sliding";

        String filterJson = "null";
        if (RandomUtils.RANDOM.nextBoolean()) {
            int tagIdx = RandomUtils.RANDOM.nextInt(RuleConfig.TAG_FIELDS.length);
            String tagField = RuleConfig.TAG_FIELDS[tagIdx];
            String tagValue = RandomUtils.randomElement(RuleConfig.TAG_VALUES[tagIdx]);
            filterJson = """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "EQ",
                      "value": "%s"
                    }""".formatted(tagField, tagValue);
        }

        return """
                {
                  "type": "AGGREGATION",
                  "field": "event_id",
                  "function": "COUNT",
                  "event_type": "%s",
                  "window": {
                    "type": "%s",
                    "size_seconds": %d,
                    "slide_seconds": %d
                  },
                  "filter": %s,
                  "operator": "%s",
                  "value": %d
                }""".formatted(eventType, windowType, size, slide, filterJson, RandomUtils.randomElement(RuleConfig.NUMERIC_OPS),
                RandomUtils.RANDOM.nextInt(20) + 1);
    }

    private static String generateAggregationClause() {
        String function = RandomUtils.randomElement("SUM", "COUNT", "AVG", "MAX", "MIN");
        String field = RandomUtils.randomElement(RuleConfig.NUM_FIELDS); 
        int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
        int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
        String windowType = size == slide ? "tumbling" : "sliding";

        String filterJson = "null";
        if (RandomUtils.RANDOM.nextBoolean()) {
            int tagIdx = RandomUtils.RANDOM.nextInt(RuleConfig.TAG_FIELDS.length);
            String tagField = RuleConfig.TAG_FIELDS[tagIdx];
            String tagValue = RandomUtils.randomElement(RuleConfig.TAG_VALUES[tagIdx]);
            filterJson = """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "EQ",
                      "value": "%s"
                    }""".formatted(tagField, tagValue);
        }

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
                  "filter": %s,
                  "operator": "%s",
                  "value": %d
                }""".formatted(field, function, windowType, size, slide, filterJson, RandomUtils.randomElement(RuleConfig.NUMERIC_OPS),
                RandomUtils.RANDOM.nextInt(10_000) + 1);
    }

    private static String generateRawFieldClause() {
        double r = RandomUtils.RANDOM.nextDouble();

        if (r < 0.40) {
            // String exact match (Tag)
            int tagIdx = RandomUtils.RANDOM.nextInt(RuleConfig.TAG_FIELDS.length);
            String field = RuleConfig.TAG_FIELDS[tagIdx];
            String[] pool = RuleConfig.TAG_VALUES[tagIdx];

            double opRoll = RandomUtils.RANDOM.nextDouble();
            if (opRoll < 0.5) {
                return """
                        {
                          "type": "RAW_FIELD",
                          "field": "%s",
                          "operator": "EQ",
                          "value": "%s"
                        }""".formatted(field, RandomUtils.randomElement(pool));
            } else if (opRoll < 0.8) {
                int count = 2 + RandomUtils.RANDOM.nextInt(3);
                Set<String> vals = new LinkedHashSet<>();
                while (vals.size() < count && vals.size() < pool.length)
                    vals.add(RandomUtils.randomElement(pool));
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
                        }""".formatted(field, RandomUtils.randomElement(pool));
            }

        } else if (r < 0.75) {
            // Numeric field
            String field = RandomUtils.randomElement(RuleConfig.NUM_FIELDS);
            if (RandomUtils.RANDOM.nextDouble() < 0.2) {
                int lo = RandomUtils.RANDOM.nextInt(5000) + 1;
                int hi = lo + RandomUtils.RANDOM.nextInt(5000) + 1;
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
                        }""".formatted(field, RandomUtils.randomElement(RuleConfig.NUMERIC_OPS), RandomUtils.RANDOM.nextInt(10_000) + 1);
            }

        } else if (r < 0.90) {
            // Boolean field
            String field = RandomUtils.randomElement("opt_in_email", "opt_in_sms", "opt_in_push", "is_3ds_verified",
                    "billing_zip_match");
            return """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "EQ",
                      "value": %s
                    }""".formatted(field, RandomUtils.RANDOM.nextBoolean());

        } else {
            // String CONTAINS
            String field = RandomUtils.randomElement(
                    "user_email", "user_first_name", "user_last_name", "product_name");
            return """
                    {
                      "type": "RAW_FIELD",
                      "field": "%s",
                      "operator": "CONTAINS",
                      "value": "example"
                    }""".formatted(field);
        }
    }
}
