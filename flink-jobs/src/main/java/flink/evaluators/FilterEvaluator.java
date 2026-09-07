package flink.evaluators;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Iterator;

/**
 * Evaluates recursive filter condition trees (LogicalNode and LeafNode/RAW_FIELD)
 * against an event's JSON payload.
 */
public class FilterEvaluator {

    /**
     * Evaluates a filter condition node against an event JSON node.
     * Returns true if the filter is null, empty, or satisfies the condition.
     */
    public static boolean evaluate(JsonNode filterNode, JsonNode eventNode) {
        if (filterNode == null || filterNode.isNull() || filterNode.isMissingNode()) {
            return true;
        }

        // Handle Logical Nodes (AND, OR, NOT)
        if (filterNode.has("operator")) {
            String op = filterNode.get("operator").asText().trim().toUpperCase();
            if ("AND".equals(op)) {
                JsonNode children = filterNode.get("children");
                if (children != null && children.isArray()) {
                    for (JsonNode child : children) {
                        if (!evaluate(child, eventNode)) {
                            return false;
                        }
                    }
                }
                return true;
            } else if ("OR".equals(op)) {
                JsonNode children = filterNode.get("children");
                if (children != null && children.isArray()) {
                    for (JsonNode child : children) {
                        if (evaluate(child, eventNode)) {
                            return true;
                        }
                    }
                }
                return false;
            } else if ("NOT".equals(op)) {
                JsonNode children = filterNode.get("children");
                if (children != null && children.isArray() && children.size() > 0) {
                    return !evaluate(children.get(0), eventNode);
                }
                return true;
            }
        }

        // Handle Leaf Node / RAW_FIELD
        String type = filterNode.has("type") ? filterNode.get("type").asText() : "";
        if ("RAW_FIELD".equalsIgnoreCase(type) || filterNode.has("field")) {
            String field = filterNode.has("field") ? filterNode.get("field").asText() : "";
            String op = filterNode.has("operator") ? filterNode.get("operator").asText() : "EQ";
            JsonNode targetValue = filterNode.get("value");

            if (field.isEmpty()) {
                return true;
            }

            if (eventNode == null || !eventNode.has(field) || eventNode.get(field).isNull()) {
                // If field is missing, only negative conditions evaluate to true
                String upperOp = op.trim().toUpperCase();
                return "NEQ".equals(upperOp) || "!=".equals(upperOp) || "NOT IN".equals(upperOp);
            }

            JsonNode actualValue = eventNode.get(field);
            return compare(actualValue, op, targetValue);
        }

        return true;
    }

    /**
     * Compares actual field value with target condition value based on operator.
     */
    public static boolean compare(JsonNode actual, String operator, JsonNode target) {
        if (actual == null || operator == null) {
            return false;
        }

        String op = operator.trim().toUpperCase();

        switch (op) {
            case "EQ":
            case "=":
                return compareEquals(actual, target);

            case "NEQ":
            case "!=":
                return !compareEquals(actual, target);

            case "GT":
            case ">":
                return compareMagnitude(actual, target) > 0;

            case "GTE":
            case ">=":
                return compareMagnitude(actual, target) >= 0;

            case "LT":
            case "<":
                return compareMagnitude(actual, target) < 0;

            case "LTE":
            case "<=":
                return compareMagnitude(actual, target) <= 0;

            case "IN":
                return checkIn(actual, target);

            case "NOT IN":
                return !checkIn(actual, target);

            case "BETWEEN":
                return checkBetween(actual, target);

            case "CONTAINS":
                return checkContains(actual, target);

            default:
                return compareEquals(actual, target);
        }
    }

    private static boolean compareEquals(JsonNode actual, JsonNode target) {
        if (target == null || target.isNull()) {
            return actual == null || actual.isNull();
        }
        if (actual.isNumber() && target.isNumber()) {
            return Double.compare(actual.asDouble(), target.asDouble()) == 0;
        }
        if (actual.isBoolean() && target.isBoolean()) {
            return actual.asBoolean() == target.asBoolean();
        }
        return actual.asText().equals(target.asText());
    }

    private static int compareMagnitude(JsonNode actual, JsonNode target) {
        if (actual.isNumber() && target.isNumber()) {
            return Double.compare(actual.asDouble(), target.asDouble());
        }

        // Try parsing timestamps (ISO-8601)
        if (actual.isTextual() && target.isTextual()) {
            try {
                Instant tActual = Instant.parse(actual.asText());
                Instant tTarget = Instant.parse(target.asText());
                return tActual.compareTo(tTarget);
            } catch (Exception ignored) {
            }
        }

        // Try parsing numbers from text
        try {
            double dActual = Double.parseDouble(actual.asText());
            double dTarget = Double.parseDouble(target.asText());
            return Double.compare(dActual, dTarget);
        } catch (NumberFormatException ignored) {
        }

        // Fallback to lexicographical comparison
        return actual.asText().compareTo(target.asText());
    }

    private static boolean checkIn(JsonNode actual, JsonNode targetArray) {
        if (targetArray == null || !targetArray.isArray()) {
            return false;
        }
        Iterator<JsonNode> iter = targetArray.elements();
        while (iter.hasNext()) {
            if (compareEquals(actual, iter.next())) {
                return true;
            }
        }
        return false;
    }

    private static boolean checkBetween(JsonNode actual, JsonNode targetRange) {
        if (targetRange == null || !targetRange.isArray() || targetRange.size() < 2) {
            return false;
        }
        JsonNode low = targetRange.get(0);
        JsonNode high = targetRange.get(1);
        return compareMagnitude(actual, low) >= 0 && compareMagnitude(actual, high) <= 0;
    }

    private static boolean checkContains(JsonNode actual, JsonNode target) {
        if (actual == null || target == null) {
            return false;
        }
        return actual.asText().contains(target.asText());
    }
}

