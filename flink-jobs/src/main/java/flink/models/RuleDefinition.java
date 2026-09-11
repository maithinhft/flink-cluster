package flink.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RuleDefinition implements Serializable {
    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String name;
    private long cooldownSeconds;
    private long version;
    private boolean enabled;
    private Set<String> triggerEvents;
    private transient JsonNode conditionNode;
    private String conditionJson;

    public RuleDefinition() {
        this.triggerEvents = new HashSet<>();
    }

    public RuleDefinition(String ruleId, String name, long cooldownSeconds, long version, boolean enabled,
                          Set<String> triggerEvents, JsonNode conditionNode, String conditionJson) {
        this.ruleId = ruleId;
        this.name = name;
        this.cooldownSeconds = cooldownSeconds;
        this.version = version;
        this.enabled = enabled;
        this.triggerEvents = triggerEvents != null ? triggerEvents : new HashSet<>();
        this.conditionNode = conditionNode;
        this.conditionJson = conditionJson;
    }

    public static RuleDefinition fromCdcJson(JsonNode cdcNode, ObjectMapper mapper) {
        if (cdcNode == null || !cdcNode.has("rule_id")) {
            return null;
        }

        String ruleId = cdcNode.get("rule_id").asText();
        String name = cdcNode.has("name") ? cdcNode.get("name").asText() : ruleId;
        long cooldown = cdcNode.has("cooldown_seconds") ? cdcNode.get("cooldown_seconds").asLong() : 0L;
        long version = cdcNode.has("version") ? cdcNode.get("version").asLong() : 1L;
        boolean enabled = !cdcNode.has("enabled") || cdcNode.get("enabled").asBoolean();

        Set<String> triggerEvents = new HashSet<>();
        JsonNode conditionNode = null;
        String conditionJson = null;

        if (cdcNode.has("rule_json")) {
            JsonNode ruleJsonNode = cdcNode.get("rule_json");
            if (ruleJsonNode.isTextual()) {
                try {
                    ruleJsonNode = mapper.readTree(ruleJsonNode.asText());
                } catch (Exception ignored) {
                }
            }

            if (ruleJsonNode != null && ruleJsonNode.isObject()) {
                parseTriggerEvents(ruleJsonNode.get("trigger_events"), triggerEvents);
                parseTriggerEvents(ruleJsonNode.get("trigger_event"), triggerEvents);

                if (ruleJsonNode.has("condition")) {
                    conditionNode = ruleJsonNode.get("condition");
                    conditionJson = conditionNode.toString();
                } else {
                    conditionNode = ruleJsonNode;
                    conditionJson = conditionNode.toString();
                }
            }
        }

        parseTriggerEvents(cdcNode.get("trigger_events"), triggerEvents);
        parseTriggerEvents(cdcNode.get("trigger_event"), triggerEvents);

        if (conditionNode == null) {
            if (cdcNode.has("condition")) {
                conditionNode = cdcNode.get("condition");
                conditionJson = conditionNode.toString();
            }
        }

        return new RuleDefinition(ruleId, name, cooldown, version, enabled, triggerEvents, conditionNode, conditionJson);
    }

    private static void parseTriggerEvents(JsonNode node, Set<String> targetSet) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                if (item.isTextual()) {
                    String val = item.asText().trim();
                    if (!val.isEmpty()) {
                        targetSet.add(val.toLowerCase());
                    }
                }
            }
        } else if (node.isTextual()) {
            String text = node.asText().trim();
            if (!text.isEmpty()) {
                if (text.contains(",")) {
                    for (String part : text.split(",")) {
                        String val = part.trim();
                        if (!val.isEmpty()) {
                            targetSet.add(val.toLowerCase());
                        }
                    }
                } else {
                    targetSet.add(text.toLowerCase());
                }
            }
        }
    }

    public boolean matchesTriggerEvent(String eventType) {
        if (triggerEvents == null || triggerEvents.isEmpty()) {
            return false;
        }
        if (eventType == null) {
            return false;
        }
        return triggerEvents.contains(eventType.trim().toLowerCase());
    }

    public JsonNode getOrParseCondition(ObjectMapper mapper) {
        if (conditionNode == null && conditionJson != null) {
            try {
                conditionNode = mapper.readTree(conditionJson);
            } catch (Exception ignored) {
            }
        }
        return conditionNode;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCooldownSeconds() {
        return cooldownSeconds;
    }

    public void setCooldownSeconds(long cooldownSeconds) {
        this.cooldownSeconds = cooldownSeconds;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Set<String> getTriggerEvents() {
        return triggerEvents;
    }

    public void setTriggerEvents(Set<String> triggerEvents) {
        this.triggerEvents = triggerEvents != null ? triggerEvents : new HashSet<>();
    }

    public JsonNode getConditionNode() {
        return conditionNode;
    }

    public void setConditionNode(JsonNode conditionNode) {
        this.conditionNode = conditionNode;
        if (conditionNode != null) {
            this.conditionJson = conditionNode.toString();
        }
    }

    public String getConditionJson() {
        return conditionJson;
    }

    public void setConditionJson(String conditionJson) {
        this.conditionJson = conditionJson;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleDefinition that = (RuleDefinition) o;
        return cooldownSeconds == that.cooldownSeconds &&
                version == that.version &&
                enabled == that.enabled &&
                Objects.equals(ruleId, that.ruleId) &&
                Objects.equals(name, that.name) &&
                Objects.equals(triggerEvents, that.triggerEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, name, cooldownSeconds, version, enabled, triggerEvents);
    }

    @Override
    public String toString() {
        return "RuleDefinition{" +
                "ruleId='" + ruleId + '\'' +
                ", name='" + name + '\'' +
                ", version=" + version +
                ", enabled=" + enabled +
                ", triggers=" + triggerEvents +
                '}';
    }
}

