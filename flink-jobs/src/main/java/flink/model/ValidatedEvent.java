package flink.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema-validated event passed from the validation operator to the keyed engine.
 * The JSON is parsed exactly once; only fields referenced by rules/specs are carried (projection),
 * stored in parallel arrays so Flink can serialize it as a POJO without Kryo.
 */
public class ValidatedEvent {

    public static final byte KIND_NULL = 0;
    public static final byte KIND_STRING = 1;
    public static final byte KIND_NUMBER = 2;
    public static final byte KIND_BOOLEAN = 3;

    public String entityId;
    public String eventId;
    public String eventType;
    public String sourceSystem;
    public long eventTimeMs;
    public long ingestTimeMs;
    public String[] names;
    public byte[] kinds;
    public String[] strs;
    public double[] nums;
    public String rawJson;

    public ValidatedEvent() {
    }

    public int find(String field) {
        if (names == null) {
            return -1;
        }
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(field)) {
                return i;
            }
        }
        return -1;
    }

    public byte kind(int i) {
        return kinds[i];
    }

    public String str(int i) {
        return strs[i];
    }

    public double num(int i) {
        return nums[i];
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final ValidatedEvent ev = new ValidatedEvent();
        private final List<String> names = new ArrayList<>();
        private final List<Byte> kinds = new ArrayList<>();
        private final List<String> strs = new ArrayList<>();
        private final List<Double> nums = new ArrayList<>();

        public Builder entity(String entityId) {
            ev.entityId = entityId;
            return this;
        }

        public Builder eventId(String eventId) {
            ev.eventId = eventId;
            return this;
        }

        public Builder eventType(String eventType) {
            ev.eventType = eventType;
            return this;
        }

        public Builder source(String sourceSystem) {
            ev.sourceSystem = sourceSystem;
            return this;
        }

        public Builder time(long eventTimeMs) {
            ev.eventTimeMs = eventTimeMs;
            return this;
        }

        public Builder ingest(long ingestTimeMs) {
            ev.ingestTimeMs = ingestTimeMs;
            return this;
        }

        public Builder raw(String rawJson) {
            ev.rawJson = rawJson;
            return this;
        }

        public Builder str(String name, String value) {
            return put(name, value == null ? KIND_NULL : KIND_STRING, value, 0.0);
        }

        public Builder num(String name, double value) {
            return put(name, KIND_NUMBER, null, value);
        }

        public Builder bool(String name, boolean value) {
            return put(name, KIND_BOOLEAN, null, value ? 1.0 : 0.0);
        }

        public Builder put(String name, byte kind, String s, double n) {
            names.add(name);
            kinds.add(kind);
            strs.add(s);
            nums.add(n);
            return this;
        }

        public ValidatedEvent build() {
            int n = names.size();
            ev.names = names.toArray(new String[0]);
            ev.kinds = new byte[n];
            ev.strs = new String[n];
            ev.nums = new double[n];
            for (int i = 0; i < n; i++) {
                ev.kinds[i] = kinds.get(i);
                ev.strs[i] = strs.get(i);
                ev.nums[i] = nums.get(i);
            }
            if (ev.eventType == null) {
                int i = ev.find("event_type");
                if (i >= 0 && ev.kinds[i] == KIND_STRING) {
                    ev.eventType = ev.strs[i].toLowerCase();
                }
            }
            return ev;
        }
    }
}
