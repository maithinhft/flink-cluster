package generator.events;

import generator.events.populators.CrmPopulator;
import generator.events.populators.EcommercePopulator;
import generator.events.populators.EventPopulator;
import generator.events.populators.PaymentPopulator;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

    public static Map<String, Object> generateEvent(long eventId, String entityId, Random random, EventConfig config) {
        int sourceIdx = random.nextInt(SOURCE_EVENT_TYPES.length);
        String sourceSystem = SOURCE_EVENT_TYPES[sourceIdx][0];
        String eventType = SOURCE_EVENT_TYPES[sourceIdx][1 + random.nextInt(SOURCE_EVENT_TYPES[sourceIdx].length - 1)];

        Instant now = Instant.now();
        Instant eventTime = now.minus(random.nextInt(24 * 60), ChronoUnit.MINUTES);
        if (random.nextDouble() < config.lateEventRate) {
            eventTime = eventTime.minus(random.nextInt(120), ChronoUnit.MINUTES);
        }

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
        event.put("session_id", "sess-" + random.nextInt(1_000_000));

        EventPopulator populator = populators.get(sourceSystem);
        if (populator != null) {
            populator.populate(event, eventType, random, isDirty, dirtyType);
        }

        return event;
    }
}
