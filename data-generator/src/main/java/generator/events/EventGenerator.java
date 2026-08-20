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
    static final double DEFAULT_DIRTY_RATE = 0.05; // 5% dirty data
    static final double DEFAULT_LATE_EVENT_RATE = 0.05;
    static final int DEFAULT_NUM_ENTITIES = 100_000;
    static final double DEFAULT_DATA_SKEW = 0.5;
    static final int DEFAULT_WORKERS = 4;

    // ============================================================
    // Source systems & event types
    // ============================================================

    static final String[][] SOURCE_EVENT_TYPES = {
            {"ecommerce",     "purchase", "view_product", "add_to_cart", "checkout"},
            {"crm",           "profile_update", "support_ticket", "campaign_response"},
            {"mobile_app",    "app_open", "screen_view", "push_click", "app_crash"},
            {"web_analytics", "page_view", "click", "form_submit", "scroll_depth"},
            {"iot_device",    "sensor_reading", "device_heartbeat", "alert"},
            {"payment",       "payment_init", "payment_success", "payment_fail", "refund"}
    };

    static final String[] ENTITY_TYPES = {"customer", "device", "account"};
    
    // Geo/Network Lookups
    static final String[] REGIONS = {"ap-southeast-1", "us-east-1", "eu-west-1", "ap-northeast-1", "sa-east-1"};
    static final String[] COUNTRIES = {"VN", "US", "JP", "KR", "SG", "TH", "ID", "PH", "MY", "AU"};
    static final String[] BROWSERS = {"Chrome", "Firefox", "Safari", "Edge", "Opera"};
    static final String[] OS = {"Android", "iOS", "Windows", "macOS", "Linux"};
    static final String[] DEVICE_TYPES = {"mobile", "tablet", "desktop", "tv"};
    static final String[] PAYMENT_GATEWAYS = {"stripe", "paypal", "vnpay", "momo"};
    static final String[] PAYMENT_METHODS = {"credit_card", "wallet", "bank_transfer", "cod"};

    // ============================================================
    // Main
    // ============================================================

    public static void main(String[] args) throws Exception {

        Config config = Config.parse(args);
        validate(config);

        System.out.println("======================================================================");
        System.out.println("High Throughput Multi-Source Event Generator (Concrete Fields)");
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
    // Generate Event (Concrete Fields & Dirty Injection)
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
        int dirtyType = isDirty ? random.nextInt(4) : -1; // 0=missing, 1=type, 2=bounds, 3=format

        Map<String, Object> event = new LinkedHashMap<>();

        // Group 1: Core & Routing
        event.put("event_id", UUID.randomUUID().toString());
        event.put("event_type", eventType);
        event.put("entity_id", entityId);
        event.put("entity_type", ENTITY_TYPES[random.nextInt(ENTITY_TYPES.length)]);
        event.put("source_system", sourceSystem);
        event.put("schema_version", "1.0");
        
        if (dirtyType == 0 && random.nextBoolean()) {
            // Intentionally missing event_time
        } else {
            event.put("event_time", eventTime.toString());
        }
        
        event.put("ingestion_time", now.minus(random.nextInt(2), ChronoUnit.SECONDS).toString());
        event.put("processing_time", now.toString());
        event.put("source_id", sourceSystem + "-" + eventId);
        event.put("trace_id", UUID.randomUUID().toString());
        event.put("correlation_id", UUID.randomUUID().toString());
        event.put("session_id", "sess-" + random.nextInt(1_000_000));
        event.put("is_retry", random.nextDouble() < 0.01);
        event.put("retry_count", random.nextDouble() < 0.01 ? random.nextInt(3) + 1 : 0);

        // Group 2: Network & Geo (Common for all web/mobile/ecommerce)
        if (!sourceSystem.equals("iot_device")) {
            if (dirtyType == 3 && random.nextBoolean()) {
                event.put("ip_address", "999.999.999.999"); // Invalid format
            } else {
                event.put("ip_address", random.nextInt(1, 256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(1, 256));
            }
            event.put("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
            event.put("country_code", COUNTRIES[random.nextInt(COUNTRIES.length)]);
            event.put("region_name", REGIONS[random.nextInt(REGIONS.length)]);
            event.put("city_name", "City-" + random.nextInt(100));
            event.put("latitude", -90.0 + random.nextDouble() * 180.0);
            event.put("longitude", -180.0 + random.nextDouble() * 360.0);
            event.put("timezone", "UTC");
            event.put("isp_name", "ISP-" + random.nextInt(10));
            event.put("connection_type", randomElement(random, "wifi", "cellular"));
            event.put("vpn_detected", random.nextDouble() < 0.02);
            event.put("bot_probability", random.nextDouble() * 0.2);
        }

        // Domain-specific fields
        switch (sourceSystem) {
            case "ecommerce":
                populateEcommerce(event, random, isDirty, dirtyType);
                break;
            case "crm":
                populateCrm(event, random, isDirty, dirtyType);
                break;
            case "mobile_app":
            case "web_analytics":
                populateAnalytics(event, random, sourceSystem, isDirty, dirtyType);
                break;
            case "payment":
                populatePayment(event, random, isDirty, dirtyType);
                break;
            case "iot_device":
                populateIot(event, random, isDirty, dirtyType);
                break;
        }

        return event;
    }

    private static void populateEcommerce(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("product_id", "prod-" + random.nextInt(10000));
        event.put("product_category", randomElement(random, "electronics", "fashion", "food", "books"));
        
        if (dirtyType == 1 && random.nextBoolean()) {
            event.put("unit_price", "N/A"); // Type mismatch: string instead of double
        } else if (dirtyType == 2 && random.nextBoolean()) {
            event.put("unit_price", -50.0); // Out of bounds: negative price
        } else {
            event.put("unit_price", 10.0 + random.nextDouble() * 500.0);
        }
        
        event.put("quantity", 1 + random.nextInt(5));
        event.put("discount_amount", random.nextDouble() * 10.0);
        event.put("tax_amount", 5.0 + random.nextDouble() * 20.0);
        
        double uPrice = event.get("unit_price") instanceof Double ? (Double) event.get("unit_price") : 100.0;
        int qty = (Integer) event.get("quantity");
        event.put("total_amount", uPrice * qty);
        
        event.put("currency", "USD");
        event.put("order_id", "ord-" + random.nextInt(100000));
        event.put("order_status", randomElement(random, "created", "paid", "shipped"));
        event.put("shipping_method", randomElement(random, "standard", "express"));
        event.put("shipping_cost", random.nextDouble() * 15.0);
        event.put("merchant_id", "merch-" + random.nextInt(100));
        event.put("is_gift", random.nextDouble() < 0.05);
    }

    private static void populateCrm(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("user_first_name", "User" + random.nextInt(1000));
        event.put("user_last_name", "Name" + random.nextInt(1000));
        
        if (dirtyType == 3 && random.nextBoolean()) {
            event.put("user_email", "invalid-email-format");
        } else {
            event.put("user_email", "user" + random.nextInt(10000) + "@example.com");
        }
        
        if (dirtyType == 2 && random.nextBoolean()) {
            event.put("user_age", -5); // Out of bounds
        } else {
            event.put("user_age", 18 + random.nextInt(60));
        }
        
        event.put("user_gender", randomElement(random, "M", "F", "O"));
        event.put("account_id", "acc-" + random.nextInt(50000));
        event.put("account_status", randomElement(random, "active", "suspended"));
        event.put("subscription_tier", randomElement(random, "free", "basic", "premium"));
        event.put("loyalty_points", random.nextInt(5000));
        event.put("lifetime_value", random.nextDouble() * 2000.0);
        event.put("user_segment", randomElement(random, "new", "returning", "vip", "churn_risk"));
        event.put("utm_source", randomElement(random, "google", "facebook", "direct"));
        event.put("opt_in_email", random.nextBoolean());
    }

    private static void populateAnalytics(Map<String, Object> event, Random random, String type, boolean isDirty, int dirtyType) {
        event.put("page_url", "https://example.com/page-" + random.nextInt(100));
        event.put("bounce", random.nextDouble() < 0.4);
        event.put("time_on_page_ms", random.nextInt(300000));
        event.put("scroll_depth_percent", random.nextInt(100));
        
        event.put("browser_name", BROWSERS[random.nextInt(BROWSERS.length)]);
        event.put("os_name", OS[random.nextInt(OS.length)]);
        event.put("device_type", DEVICE_TYPES[random.nextInt(DEVICE_TYPES.length)]);
        event.put("screen_width", 1024 + random.nextInt(1000));
        event.put("screen_height", 768 + random.nextInt(1000));
        
        if (type.equals("mobile_app")) {
            event.put("app_id", "com.example.app");
            event.put("app_version", "1." + random.nextInt(10) + ".0");
            event.put("is_first_open", random.nextDouble() < 0.1);
        }
        event.put("page_load_time_ms", random.nextInt(5000));
    }

    private static void populatePayment(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("transaction_id", "txn-" + UUID.randomUUID().toString().substring(0, 8));
        event.put("payment_gateway", PAYMENT_GATEWAYS[random.nextInt(PAYMENT_GATEWAYS.length)]);
        event.put("payment_method", PAYMENT_METHODS[random.nextInt(PAYMENT_METHODS.length)]);
        event.put("card_network", randomElement(random, "visa", "mastercard", "amex"));
        event.put("card_last_four", String.format("%04d", random.nextInt(10000)));
        event.put("transaction_status", randomElement(random, "pending", "success", "failed"));
        
        if (dirtyType == 1 && random.nextBoolean()) {
            event.put("fraud_score", "HIGH"); // Type mismatch
        } else {
            event.put("fraud_score", random.nextDouble() * 100.0);
        }
        
        event.put("is_3ds_verified", random.nextBoolean());
        event.put("fee_amount", random.nextDouble() * 5.0);
        event.put("settlement_amount", random.nextDouble() * 500.0);
    }

    private static void populateIot(Map<String, Object> event, Random random, boolean isDirty, int dirtyType) {
        event.put("hardware_serial_number", "hw-" + random.nextInt(10000));
        event.put("firmware_version", "fw-2." + random.nextInt(5));
        
        if (dirtyType == 2 && random.nextBoolean()) {
            event.put("battery_level_percent", 150.0); // Out of bounds
        } else {
            event.put("battery_level_percent", random.nextDouble() * 100.0);
        }
        
        event.put("is_charging", random.nextBoolean());
        event.put("uptime_seconds", random.nextInt(1000000));
        event.put("temperature_celsius", 20.0 + random.nextDouble() * 30.0);
        event.put("humidity_percent", 30.0 + random.nextDouble() * 50.0);
        event.put("motion_detected", random.nextDouble() < 0.1);
        event.put("cpu_usage_percent", random.nextDouble() * 100.0);
        event.put("memory_usage_bytes", (long) random.nextInt(1024 * 1024 * 100));
    }

    private static String randomElement(Random random, String... elements) {
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
            for (int i = 0; i < numEntities; i++) entities[i] = "entity-" + i;
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