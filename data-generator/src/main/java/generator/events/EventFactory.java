package generator.events;

import generator.events.populators.CrmPopulator;
import generator.events.populators.EcommercePopulator;
import generator.events.populators.EventPopulator;
import generator.events.populators.PaymentPopulator;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class EventFactory {

    private static final String[][] SOURCE_EVENT_TYPES = {
            { "ecommerce", "add_to_cart", "remove_from_cart", "purchase", "order_created", "order_shipped",
                    "order_completed", "order_cancelled" },
            { "crm", "profile_update", "login", "logout", "account_created", "account_status_change",
                    "subscription_change", "support_ticket_created", "support_ticket_resolved" },
            { "payment", "payment_initiated", "payment_success", "payment_failed", "refund", "chargeback" }
    };

    private static final Map<String, EventPopulator> populators = Map.of(
            "ecommerce", new EcommercePopulator(),
            "crm", new CrmPopulator(),
            "payment", new PaymentPopulator());

    /**
     * Event time model: normal events carry "now" minus a bounded jitter (out-of-order but within the
     * job's 1 minute watermark bound); a fraction {@code lateEventRate} is delayed by
     * [lateMinMs, lateMaxMs], i.e. beyond bucket end + 1 minute tolerance, so they exercise the
     * late-data path (DLQ). Defaults keep the stream realistic for a watermark-driven job.
     */
    static Instant eventTime(Instant now, Random random, EventConfig config) {
        if (random.nextDouble() < config.lateEventRate) {
            long span = Math.max(1L, config.lateMaxMs - config.lateMinMs);
            return now.minusMillis(config.lateMinMs + (long) (random.nextDouble() * span));
        }
        if (config.maxOutOfOrderMs <= 0) {
            return now;
        }
        return now.minusMillis((long) (random.nextDouble() * config.maxOutOfOrderMs));
    }

    public static Map<String, Object> generateEvent(long eventId, String entityId, Random random, EventConfig config) {
        int sourceIdx = random.nextInt(SOURCE_EVENT_TYPES.length);
        String sourceSystem = SOURCE_EVENT_TYPES[sourceIdx][0];
        String eventType = SOURCE_EVENT_TYPES[sourceIdx][1 + random.nextInt(SOURCE_EVENT_TYPES[sourceIdx].length - 1)];

        Instant eventTime = eventTime(config.getSimulatedNow(), random, config);

        boolean isDirty = random.nextDouble() < config.dirtyRate;
        int dirtyType = isDirty ? random.nextInt(4) : -1;

        Map<String, Object> event = new LinkedHashMap<>();

        event.put("event_id", UUID.randomUUID().toString());
        event.put("event_type", eventType);
        event.put("entity_id", entityId); // customer id
        event.put("source_system", sourceSystem);
        event.put("schema_version", "1.0");

        if (dirtyType == 0 && random.nextBoolean()) {
            // Intentionally missing event_time
        } else {
            event.put("event_time", eventTime.toString());
        }

        event.put("source_id", sourceSystem + "-" + eventId);
        event.put("trace_id", UUID.randomUUID().toString());
        event.put("correlation_id", UUID.randomUUID().toString());

        EventPopulator populator = populators.get(sourceSystem);
        if (populator != null) {
            populator.populate(event, eventType, random, isDirty, dirtyType);
        }

        return event;
    }

    public static String sourceOf(String eventType) {
        for (String[] group : SOURCE_EVENT_TYPES) {
            for (int i = 1; i < group.length; i++) {
                if (group[i].equals(eventType)) {
                    return group[0];
                }
            }
        }
        return null;
    }

    /** A clean, schema-valid event of the given type at {@code eventTime} (used by the demo app). */
    public static Map<String, Object> generateEventOfType(String entityId, String eventType, Instant eventTime, Random random) {
        String sourceSystem = sourceOf(eventType);
        if (sourceSystem == null) {
            throw new IllegalArgumentException("Unknown event_type: " + eventType);
        }
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", UUID.randomUUID().toString());
        event.put("event_type", eventType);
        event.put("entity_id", entityId);
        event.put("source_system", sourceSystem);
        event.put("schema_version", "1.0");
        event.put("event_time", eventTime.toString());
        event.put("source_id", sourceSystem + "-demo");
        event.put("trace_id", UUID.randomUUID().toString());
        event.put("correlation_id", UUID.randomUUID().toString());
        populators.get(sourceSystem).populate(event, eventType, random, false, -1);
        return event;
    }
}
