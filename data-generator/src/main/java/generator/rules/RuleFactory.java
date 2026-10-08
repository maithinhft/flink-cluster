package generator.rules;

import generator.common.RandomUtils;
import java.util.LinkedHashSet;
import java.util.Set;

public class RuleFactory {

  /** Probability that the root of a condition tree is AND (realistic segment definitions; makes the index useful). */
  public static double andRootRatio = 0.7;
  /** Probability that a leaf is a LINEAR expression mixing aggregates and raw fields. */
  public static double linearRatio = 0.1;
  /** "inline" (field + filter in the rule) or "reference" (half of the aggregations reference schema metric aliases). */
  public static String metricMode = "inline";
  /** Metric aliases available for reference mode (loaded from the schema files). */
  public static java.util.List<String> metricAliases = new java.util.ArrayList<>();

  public static String generateTopLevelRuleJson(int maxDepth) {
    int numTriggers = 1 + RandomUtils.RANDOM.nextInt(3);
    Set<String> triggers = new LinkedHashSet<>();
    while (triggers.size() < numTriggers) {
      triggers.add(RandomUtils.randomElement(RuleConfig.EVENT_TYPES));
    }

    String conditionTree = generateConditionTree(maxDepth);
    String indentedCondition = conditionTree.replaceAll("(?m)^", "    ");

    String triggersJson = "\"" + String.join("\", \"", triggers) + "\"";

    return """
        {
          "trigger_events": [%s],
          "condition": %s
        }""".formatted(triggersJson, indentedCondition.trim());
  }

  public static String generateConditionTree(int maxDepth) {
    return generateNode(0, maxDepth);
  }

  private static String generateNode(int currentDepth, int maxDepth) {
    if (currentDepth >= maxDepth || (currentDepth > 0 && RandomUtils.RANDOM.nextDouble() < 0.4)) {
      return generateLeafClause();
    }

    String operator;
    double r = RandomUtils.RANDOM.nextDouble();
    if (currentDepth == 0 && RandomUtils.RANDOM.nextDouble() < andRootRatio)
      operator = "AND";
    else if (r < 0.45)
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
    if (RandomUtils.RANDOM.nextDouble() < linearRatio)
      return generateLinearClause();
    if ("reference".equalsIgnoreCase(metricMode) && !metricAliases.isEmpty() && RandomUtils.RANDOM.nextDouble() < 0.25)
      return generateAliasAggregationClause();
    double r = RandomUtils.RANDOM.nextDouble();
    if (r < 0.25)
      return generateAggregationClause();
    else if (r < 0.50)
      return generateEventCountAggregationClause();
    else if (r < 0.75)
      return generateSourceSystemCountAggregationClause();
    return generateRawFieldClause();
  }

  private static String formatAggregationResult(String field, String function, String windowType, int lookback,
      int size, int slide, String filterJson, int baseVal) {
    String operator;
    String valueStr;
    if (RandomUtils.RANDOM.nextDouble() < 0.2) {
      operator = "BETWEEN";
      int lo = RandomUtils.RANDOM.nextInt(baseVal) + 1;
      int hi = lo + RandomUtils.RANDOM.nextInt(baseVal) + 1;
      valueStr = "[" + lo + ", " + hi + "]";
    } else {
      operator = RandomUtils.randomElement(RuleConfig.NUMERIC_OPS);
      valueStr = String.valueOf(RandomUtils.RANDOM.nextInt(baseVal) + 1);
    }

    return """
        {
          "type": "AGGREGATION",
          "field": "%s",
          "function": "%s",
          "window": %s,
          "filter": %s,
          "operator": "%s",
          "value": %s
        }""".formatted(field, function, windowJson(windowType, lookback, size, slide), filterJson, operator, valueStr);
  }

  private static String generateSourceSystemCountAggregationClause() {
    String sourceSystem = RandomUtils.randomElement("ecommerce", "crm", "payment");
    int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
    int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
    String windowType = size == slide ? "tumbling" : "sliding";
    int maxMultipliers = 86400 / size;
    int lookback = size * (RandomUtils.RANDOM.nextInt(maxMultipliers) + 1);

    String finalFilterJson = """
        {
          "type": "RAW_FIELD",
          "field": "source_system",
          "operator": "EQ",
          "value": "%s"
        }""".formatted(sourceSystem);

    if (RandomUtils.RANDOM.nextBoolean()) {
      int tagIdx = RandomUtils.RANDOM.nextInt(RuleConfig.TAG_FIELDS.length);
      String tagField = RuleConfig.TAG_FIELDS[tagIdx];
      String tagValue = RandomUtils.randomElement(RuleConfig.TAG_VALUES[tagIdx]);
      String tagFilterJson = """
          {
            "type": "RAW_FIELD",
            "field": "%s",
            "operator": "EQ",
            "value": "%s"
          }""".formatted(tagField, tagValue);
      finalFilterJson = """
          {
            "operator": "AND",
            "children": [
              %s,
              %s
            ]
          }""".formatted(finalFilterJson, tagFilterJson);
    }

    return formatAggregationResult("event_id", "COUNT", windowType, lookback, size, slide, finalFilterJson, 50);
  }

  private static String generateEventCountAggregationClause() {
    String eventType = RandomUtils.randomElement(RuleConfig.EVENT_TYPES);
    int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
    int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
    String windowType = size == slide ? "tumbling" : "sliding";
    int maxMultipliers = 86400 / size;
    int lookback = size * (RandomUtils.RANDOM.nextInt(maxMultipliers) + 1);

    String finalFilterJson = """
        {
          "type": "RAW_FIELD",
          "field": "event_type",
          "operator": "EQ",
          "value": "%s"
        }""".formatted(eventType);

    if (RandomUtils.RANDOM.nextBoolean()) {
      int tagIdx = RandomUtils.RANDOM.nextInt(RuleConfig.TAG_FIELDS.length);
      String tagField = RuleConfig.TAG_FIELDS[tagIdx];
      String tagValue = RandomUtils.randomElement(RuleConfig.TAG_VALUES[tagIdx]);
      String tagFilterJson = """
          {
            "type": "RAW_FIELD",
            "field": "%s",
            "operator": "EQ",
            "value": "%s"
          }""".formatted(tagField, tagValue);
      finalFilterJson = """
          {
            "operator": "AND",
            "children": [
              %s,
              %s
            ]
          }""".formatted(finalFilterJson, tagFilterJson);
    }

    return formatAggregationResult("event_id", "COUNT", windowType, lookback, size, slide, finalFilterJson, 20);
  }

  private static String generateAggregationClause() {
    String function = RandomUtils.randomElement("SUM", "COUNT", "AVG", "MAX", "MIN");
    String field = RandomUtils.randomElement(RuleConfig.NUM_FIELDS);
    int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
    int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
    String windowType = size == slide ? "tumbling" : "sliding";
    int maxMultipliers = 86400 / size;
    int lookback = size * (RandomUtils.RANDOM.nextInt(maxMultipliers) + 1);

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

    return formatAggregationResult(field, function, windowType, lookback, size, slide, filterJson, 10_000);
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
            }""".formatted(field, RandomUtils.randomElement(RuleConfig.NUMERIC_OPS),
            RandomUtils.RANDOM.nextInt(10_000) + 1);
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

  static String windowJson(String windowType, int lookback, int size, int slide) {
    String quantifier = "";
    if (lookback > size && RandomUtils.RANDOM.nextDouble() < 0.3)
      quantifier = ", \"quantifier\": \"" + RandomUtils.randomElement("ANY", "ALL", "LATEST") + "\"";
    return "{\"type\": \"%s\", \"lookback_seconds\": %d, \"size_seconds\": %d, \"slide_seconds\": %d%s}"
        .formatted(windowType, lookback, size, slide, quantifier);
  }

  private static String randomWindow() {
    int size = RandomUtils.randomInt(300, 3600, 21600, 86400);
    int slide = size == 86400 ? 300 : (size == 3600 ? 300 : size);
    String windowType = size == slide ? "tumbling" : "sliding";
    int lookback = size * (RandomUtils.RANDOM.nextInt(86400 / size) + 1);
    return windowJson(windowType, lookback, size, slide);
  }

  private static String generateAliasAggregationClause() {
    String alias = metricAliases.get(RandomUtils.RANDOM.nextInt(metricAliases.size()));
    String operator = RandomUtils.randomElement(RuleConfig.NUMERIC_OPS);
    return """
        {
          "type": "AGGREGATION",
          "metric_id": "%s",
          "window": %s,
          "operator": "%s",
          "value": %d
        }""".formatted(alias, randomWindow(), operator, RandomUtils.RANDOM.nextInt(20) + 1);
  }

  /** sum(total_amount) - k * count(refund) + c * raw numeric field  OP  threshold */
  private static String generateLinearClause() {
    String rawField = RandomUtils.randomElement(RuleConfig.NUM_FIELDS);
    int k = 1000 * (1 + RandomUtils.RANDOM.nextInt(10));
    int c = 1 + RandomUtils.RANDOM.nextInt(10);
    String window = randomWindow();
    return """
        {
          "type": "LINEAR",
          "terms": [
            {"coef": 1, "ref": {"type": "AGGREGATION", "field": "total_amount", "function": "SUM", "window": %s}},
            {"coef": -%d, "ref": {"type": "AGGREGATION", "field": "event_id", "function": "COUNT", "window": %s,
              "filter": {"type": "RAW_FIELD", "field": "event_type", "operator": "EQ", "value": "refund"}}},
            {"coef": %d, "field": "%s"}
          ],
          "constant": 0,
          "operator": "%s",
          "value": %d
        }""".formatted(window, k, window, c, rawField, RandomUtils.randomElement(RuleConfig.NUMERIC_OPS),
        RandomUtils.RANDOM.nextInt(50_000) + 1);
  }

  /** Loads all metric ids from schema JSON files (for --rule-metric-mode reference). */
  public static void loadMetricAliases(java.nio.file.Path schemaDir) throws java.io.IOException {
    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    java.util.List<String> ids = new java.util.ArrayList<>();
    try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(schemaDir)) {
      for (java.nio.file.Path p : (Iterable<java.nio.file.Path>) files.filter(f -> f.toString().endsWith(".json")).sorted()::iterator) {
        com.fasterxml.jackson.databind.JsonNode metrics = mapper.readTree(p.toFile()).path("metrics");
        for (com.fasterxml.jackson.databind.JsonNode m : metrics) {
          if (m.hasNonNull("metric_id")) ids.add(m.get("metric_id").asText());
        }
      }
    }
    metricAliases = ids;
  }
}
