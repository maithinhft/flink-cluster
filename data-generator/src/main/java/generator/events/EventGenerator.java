package generator.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class EventGenerator {

    // ============================================================
    // Configuration defaults
    // ============================================================

    static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    static final String DEFAULT_TOPIC = "events";
    static final long DEFAULT_NUM_EVENTS = 1_000_000L;
    static final double DEFAULT_DIRTY_RATE = 0.05;
    static final double DEFAULT_LATE_EVENT_RATE = 0.05;
    static final int DEFAULT_NUM_ENTITIES = 100_000;
    static final double DEFAULT_DATA_SKEW = 0.5;
    static final int DEFAULT_WORKERS = 4;

    // ============================================================
    // Source systems & event types
    // ============================================================

    static final String[][] SOURCE_EVENT_TYPES = {
            {"ecommerce", "add_to_cart", "remove_from_cart", "purchase", "order_created", "order_shipped", "order_completed", "order_cancelled"},
            {"crm", "profile_update", "login", "logout", "account_created", "account_status_change", "subscription_change", "support_ticket_created", "support_ticket_resolved"},
            {"payment", "payment_initiated", "payment_success", "payment_failed", "refund", "chargeback"}
    };
    
    // Lookups
    static final String[] COUNTRIES = {"VN", "US", "JP", "KR", "SG"};
    static final String[] PAYMENT_GATEWAYS = {"stripe", "paypal", "vnpay", "momo"};
    static final String[] PAYMENT_METHODS = {"credit_card", "wallet", "bank_transfer"};
    static final String[] CARD_NETWORKS = {"visa", "mastercard", "amex", "napas"};

    // ============================================================
    // Main
    // ============================================================

    public static void main(String[] args) throws Exception {

        Config config = Config.parse(args);
        validate(config);

        System.out.println("======================================================================");
        System.out.println("High Throughput Multi-Source Event Generator");
        System.out.println("======================================================================");
        System.out.printf("Kafka             : %s%n", config.bootstrapServers);
        System.out.printf("Topic             : %s%n", config.topic);
        System.out.printf("Events            : %,d%n", config.numEvents);
        System.out.printf("Dirty data rate   : %.2f%%%n", config.dirtyRate * 100);
        System.out.printf("Entities          : %,d%n", config.numEntities);
        System.out.printf("Data skew         : %.2f%n", config.dataSkew);
        System.out.printf("Late event rate   : %.2f%%%n", config.lateEventRate * 100);
        System.out.printf("Workers           : %d%n", config.workers);
        System.out.println("======================================================================");

        EntityPool entityPool = new EntityPool(config.numEntities, config.dataSkew);
        long base = config.numEvents / config.workers;
        long remainder = config.numEvents % config.workers;
        CountDownLatch latch = new CountDownLatch(config.workers);
        AtomicLong totalSent = new AtomicLong(0);
        Thread[] threads = new Thread[config.workers];
        long currentStartId = 0;
        long globalStart = System.nanoTime();

        for (int workerId = 0; workerId < config.workers; workerId++) {
            long workerEvents = base + (workerId < remainder ? 1 : 0);
            long startId = currentStartId;
            currentStartId += workerEvents;

            Worker worker = new Worker(workerId, startId, workerEvents, config, entityPool, latch, totalSent);
            threads[workerId] = new Thread(worker, "event-generator-" + workerId);
            threads[workerId].start();
        }

        latch.await();

        double elapsed = (System.nanoTime() - globalStart) / 1_000_000_000.0;
        double throughput = totalSent.get() / elapsed;

        System.out.println();
        System.out.println("======================================================================");
        System.out.println("Benchmark Result");
        System.out.println("======================================================================");
        System.out.printf("Total events       : %,d%n", totalSent.get());
        System.out.printf("Elapsed            : %.2f s%n", elapsed);
        System.out.printf("Kafka throughput   : %,.0f events/sec%n", throughput);
        System.out.println("======================================================================");
    }

    // ============================================================
    // Worker
    // ============================================================

    static class Worker implements Runnable {
        private final int workerId;
        private final long startEventId;
        private final long numEvents;
        private final Config config;
        private final EntityPool entityPool;
        private final CountDownLatch latch;
        private final AtomicLong totalSent;

        Worker(int workerId, long startEventId, long numEvents, Config config, EntityPool entityPool, CountDownLatch latch, AtomicLong totalSent) {
            this.workerId = workerId;
            this.startEventId = startEventId;
            this.numEvents = numEvents;
            this.config = config;
            this.entityPool = entityPool;
            this.latch = latch;
            this.totalSent = totalSent;
        }

        @Override
        public void run() {
            KafkaProducer<String, byte[]> producer = null;
            try {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                props.put(ProducerConfig.ACKS_CONFIG, "1");
                props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
                props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);
                props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
                props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024L);

                producer = new KafkaProducer<>(props);
                ObjectMapper mapper = new ObjectMapper();
                Random random = new Random(12345L + workerId);
                long start = System.nanoTime();
                long sent = 0;

                for (long i = 0; i < numEvents; i++) {
                    long eventId = startEventId + i;
                    String entityId = entityPool.next(random);
                    Map<String, Object> event = generateEvent(eventId, entityId, random, config);
                    byte[] json = mapper.writeValueAsBytes(event);
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(config.topic, entityId, json);
                    producer.send(record);
                    sent++;
                }

                producer.flush();
                double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;
                totalSent.addAndGet(sent);
                System.out.printf("Worker %d: %,d events in %.2fs (%,.0f events/s)%n", workerId, sent, elapsed, sent / elapsed);

            } catch (Exception e) {
                System.err.printf("Worker %d failed: %s%n", workerId, e.getMessage());
                e.printStackTrace();
            } finally {
                if (producer != null) producer.close();
                latch.countDown();
            }
        }
    }

    // ============================================================
    // Generate Event
    // ============================================================

    static Map<String, Object> generateEvent(long eventId, String entityId, Random random, Config config) {
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
        event.put("schema_version", "1.1");
        
        if (dirtyType == 0 && random.nextBoolean()) {
            // Intentionally missing event_time
        } else {
            event.put("event_time", eventTime.toString());
        }
        
        event.put("source_id", sourceSystem + "-" + eventId);
        event.put("trace_id", UUID.randomUUID().toString());
        event.put("correlation_id", UUID.randomUUID().toString());
        event.put("session_id", "sess-" + random.nextInt(1_000_000));

        switch (sourceSystem) {
            case "ecommerce":
                populateEcommerce(event, random, isDirty, dirtyType);
                break;
            case "crm":
                populateCrm(event, random, isDirty, dirtyType);
                break;
            case "payment":
                populatePayment(event, random, isDirty, dirtyType);
                break;
        }

        return event;
    }

    private static void populateEcommerce(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("product_id", "prod-" + random.nextInt(10000));
        event.put("product_name", "Product " + random.nextInt(1000));
        event.put("product_category", randomElement(random, "electronics", "fashion", "food", "books"));
        event.put("product_subcategory", "subcat-" + random.nextInt(10));
        event.put("product_sku", "SKU-" + random.nextInt(10000));
        event.put("product_brand", "Brand-" + random.nextInt(20));
        
        if (dirtyType == 1 && random.nextBoolean()) {
            event.put("unit_price", "N/A"); // Type mismatch
        } else if (dirtyType == 2 && random.nextBoolean()) {
            event.put("unit_price", -50.0); // Out of bounds
        } else {
            event.put("unit_price", 10.0 + random.nextDouble() * 500.0);
        }
        
        event.put("quantity", 1 + random.nextInt(5));
        event.put("discount_amount", random.nextDouble() * 10.0);
        event.put("discount_code", "SUMMER" + random.nextInt(100));
        event.put("tax_amount", 5.0 + random.nextDouble() * 20.0);
        
        double uPrice = event.get("unit_price") instanceof Double ? (Double) event.get("unit_price") : 100.0;
        int qty = (Integer) event.get("quantity");
        event.put("total_amount", uPrice * qty);
        
        event.put("currency", randomElement(random, "USD", "VND"));
        event.put("cart_id", "cart-" + random.nextInt(100000));
        event.put("order_id", "ord-" + random.nextInt(100000));
        event.put("order_status", randomElement(random, "created", "paid", "shipped", "completed", "cancelled"));
        event.put("shipping_method", randomElement(random, "standard", "express"));
        event.put("shipping_cost", random.nextDouble() * 15.0);
        
        event.put("delivery_address_line1", random.nextInt(9999) + " Main St");
        event.put("delivery_city", "City-" + random.nextInt(50));
        event.put("delivery_country", randomElement(random, COUNTRIES));
        event.put("estimated_delivery_date", Instant.now().plus(random.nextInt(7), ChronoUnit.DAYS).toString().substring(0, 10));
        
        event.put("billing_address_line1", event.get("delivery_address_line1"));
        event.put("billing_city", event.get("delivery_city"));
        event.put("billing_country", event.get("delivery_country"));
        
        event.put("merchant_id", "merch-" + random.nextInt(100));
        event.put("store_id", "store-" + random.nextInt(10));
        event.put("warranty_period_months", randomElement(random, 0, 12, 24));
    }

    private static void populateCrm(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("user_first_name", "User" + random.nextInt(1000));
        event.put("user_last_name", "Name" + random.nextInt(1000));
        
        if (dirtyType == 3 && random.nextBoolean()) {
            event.put("user_email", "invalid-email-format");
        } else {
            event.put("user_email", "user" + random.nextInt(10000) + "@example.com");
        }
        
        event.put("user_phone", "+8498" + random.nextInt(10000000));
        event.put("user_gender", randomElement(random, "M", "F", "O"));
        event.put("user_date_of_birth", "19" + (50 + random.nextInt(50)) + "-01-01");
        event.put("user_nationality", randomElement(random, COUNTRIES));
        
        event.put("account_id", "acc-" + random.nextInt(50000));
        event.put("account_status", randomElement(random, "active", "suspended", "closed"));
        event.put("registration_date", Instant.now().minus(random.nextInt(365), ChronoUnit.DAYS).toString());
        event.put("last_login_date", Instant.now().minus(random.nextInt(10), ChronoUnit.DAYS).toString());
        
        event.put("subscription_tier", randomElement(random, "free", "basic", "premium"));
        
        if (dirtyType == 2 && random.nextBoolean()) {
            event.put("loyalty_points", -100);
        } else {
            event.put("loyalty_points", random.nextInt(5000));
        }
        
        event.put("opt_in_email", random.nextBoolean());
        event.put("opt_in_sms", random.nextBoolean());
        event.put("opt_in_push", random.nextBoolean());
        
        event.put("support_ticket_id", "tck-" + random.nextInt(10000));
        event.put("support_ticket_status", randomElement(random, "open", "in_progress", "resolved"));
        event.put("satisfaction_score", 1 + random.nextInt(5));
    }

    private static void populatePayment(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("transaction_id", "txn-" + UUID.randomUUID().toString().substring(0, 8));
        event.put("payment_gateway", PAYMENT_GATEWAYS[random.nextInt(PAYMENT_GATEWAYS.length)]);
        event.put("payment_method", PAYMENT_METHODS[random.nextInt(PAYMENT_METHODS.length)]);
        event.put("card_network", randomElement(random, CARD_NETWORKS));
        event.put("bank_name", "Bank-" + random.nextInt(10));
        event.put("account_number_hash", "hash-" + random.nextInt(999999));
        
        event.put("transaction_status", randomElement(random, "pending", "success", "failed"));
        event.put("payment_error_message", random.nextDouble() < 0.1 ? "Insufficient funds" : null);
        
        event.put("is_3ds_verified", random.nextBoolean());
        event.put("billing_zip_match", random.nextDouble() < 0.9);
    }

    private static String randomElement(Random random, String... elements) {
        return elements[random.nextInt(elements.length)];
    }
    
    private static int randomElement(Random random, int... elements) {
        return elements[random.nextInt(elements.length)];
    }

    // ============================================================
    // Config and Entities
    // ============================================================

    static class EntityPool {
        private final String[] entities;
        private final int hotEntityCount;
        private final double skew;

        EntityPool(int numEntities, double skew) {
            entities = new String[numEntities];
            for (int i = 0; i < numEntities; i++) entities[i] = "customer-" + i;
            hotEntityCount = Math.max(1, (int) (numEntities * 0.01));
            this.skew = skew;
        }

        String next(Random random) {
            if (skew > 0 && random.nextDouble() < skew) return entities[random.nextInt(hotEntityCount)];
            return entities[random.nextInt(entities.length)];
        }
    }

    static class Config {
        String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        String topic = DEFAULT_TOPIC;
        long numEvents = DEFAULT_NUM_EVENTS;
        double dirtyRate = DEFAULT_DIRTY_RATE;
        double lateEventRate = DEFAULT_LATE_EVENT_RATE;
        int numEntities = DEFAULT_NUM_ENTITIES;
        double dataSkew = DEFAULT_DATA_SKEW;
        int workers = DEFAULT_WORKERS;

        static Config parse(String[] args) {
            Config config = new Config();
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--bootstrap-servers": config.bootstrapServers = args[++i]; break;
                    case "--topic": config.topic = args[++i]; break;
                    case "--num-events": config.numEvents = Long.parseLong(args[++i]); break;
                    case "--dirty-rate": config.dirtyRate = Double.parseDouble(args[++i]); break;
                    case "--late-event-rate": config.lateEventRate = Double.parseDouble(args[++i]); break;
                    case "--num-entities": config.numEntities = Integer.parseInt(args[++i]); break;
                    case "--data-skew": config.dataSkew = Double.parseDouble(args[++i]); break;
                    case "--workers": config.workers = Integer.parseInt(args[++i]); break;
                }
            }
            return config;
        }
    }

    static void validate(Config config) {
        if (config.numEvents <= 0) throw new IllegalArgumentException("num-events must be > 0");
        if (config.dirtyRate < 0 || config.dirtyRate > 1) throw new IllegalArgumentException("dirty-rate must be 0-1");
        if (config.lateEventRate < 0 || config.lateEventRate > 1) throw new IllegalArgumentException("late-event-rate must be 0-1");
    }
}