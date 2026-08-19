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
    // Configuration
    // ============================================================

    static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    static final String DEFAULT_TOPIC = "events";

    static final long DEFAULT_NUM_EVENTS = 1_000_000L;

    static final int DEFAULT_NUM_ATTRIBUTES = 20;

    static final double DEFAULT_LATE_EVENT_RATE = 0.05;

    static final int DEFAULT_NUM_ENTITIES = 100_000;

    static final double DEFAULT_DATA_SKEW = 0.5;

    static final int DEFAULT_WORKERS = 4;

    // ============================================================
    // Static pools
    // ============================================================

    static final String[] EVENT_TYPES = {
            "purchase",
            "view_product",
            "add_to_cart",
            "remove_from_cart",
            "login"
    };

    static final String[] ENTITY_TYPES = {
            "customer"
    };

    static final String[] CATEGORIES = {
            "electronics",
            "fashion",
            "food",
            "books",
            "home",
            "sports"
    };

    static final String[] CITIES = {
            "Hanoi",
            "HoChiMinh",
            "DaNang",
            "HaiPhong",
            "CanTho"
    };

    static final String[] DEVICES = {
            "mobile",
            "desktop",
            "tablet"
    };

    static final String[] PAYMENT_METHODS = {
            "cash",
            "card",
            "bank_transfer",
            "e_wallet"
    };

    static final String[] SOURCES = {
            "google",
            "facebook",
            "direct",
            "recommendation"
    };

    static final String[] LOGIN_METHODS = {
            "password",
            "google",
            "facebook",
            "apple"
    };

    // ============================================================
    // Main
    // ============================================================

    public static void main(String[] args)
            throws Exception {

        Config config = Config.parse(args);

        validate(config);

        System.out.println(
                "======================================================================");

        System.out.println(
                "High Throughput Java Kafka Event Generator");

        System.out.println(
                "======================================================================");

        System.out.printf(
                "Kafka             : %s%n",
                config.bootstrapServers);

        System.out.printf(
                "Topic             : %s%n",
                config.topic);

        System.out.printf(
                "Events            : %,d%n",
                config.numEvents);

        System.out.printf(
                "Attributes/event  : %d%n",
                config.numAttributes);

        System.out.printf(
                "Entities          : %,d%n",
                config.numEntities);

        System.out.printf(
                "Data skew         : %.2f%n",
                config.dataSkew);

        System.out.printf(
                "Late event rate   : %.2f%%%n",
                config.lateEventRate * 100);

        System.out.printf(
                "Workers            : %d%n",
                config.workers);

        System.out.println(
                "======================================================================");

        // --------------------------------------------------------
        // Entity pool
        // --------------------------------------------------------

        EntityPool entityPool = new EntityPool(
                config.numEntities,
                config.dataSkew);

        System.out.println(
                "Entity pool size  : "
                        + entityPool.size());

        // --------------------------------------------------------
        // Split work
        // --------------------------------------------------------

        long base = config.numEvents
                / config.workers;

        long remainder = config.numEvents
                % config.workers;

        CountDownLatch latch = new CountDownLatch(
                config.workers);

        AtomicLong totalSent = new AtomicLong(0);

        Thread[] threads = new Thread[config.workers];

        long currentStartId = 0;

        long globalStart = System.nanoTime();

        // --------------------------------------------------------
        // Start workers
        // --------------------------------------------------------

        for (int workerId = 0; workerId < config.workers; workerId++) {

            long workerEvents = base
                    + (workerId < remainder ? 1 : 0);

            long startId = currentStartId;

            currentStartId += workerEvents;

            Worker worker = new Worker(
                    workerId,
                    startId,
                    workerEvents,
                    config,
                    entityPool,
                    latch,
                    totalSent);

            threads[workerId] = new Thread(
                    worker,
                    "event-generator-" + workerId);

            threads[workerId].start();
        }

        // --------------------------------------------------------
        // Wait
        // --------------------------------------------------------

        latch.await();

        double elapsed = (System.nanoTime() - globalStart)
                / 1_000_000_000.0;

        double throughput = totalSent.get()
                / elapsed;

        System.out.println();

        System.out.println(
                "======================================================================");

        System.out.println(
                "Benchmark Result");

        System.out.println(
                "======================================================================");

        System.out.printf(
                "Total events       : %,d%n",
                totalSent.get());

        System.out.printf(
                "Elapsed            : %.2f s%n",
                elapsed);

        System.out.printf(
                "Kafka throughput   : %,.0f events/sec%n",
                throughput);

        System.out.println(
                "======================================================================");
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

        Worker(
                int workerId,
                long startEventId,
                long numEvents,
                Config config,
                EntityPool entityPool,
                CountDownLatch latch,
                AtomicLong totalSent) {

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

                props.put(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        config.bootstrapServers);

                props.put(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringSerializer");

                props.put(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.ByteArraySerializer");

                // ------------------------------------------------
                // Throughput settings
                // ------------------------------------------------

                props.put(
                        ProducerConfig.ACKS_CONFIG,
                        "1");

                props.put(
                        ProducerConfig.COMPRESSION_TYPE_CONFIG,
                        "lz4");

                props.put(
                        ProducerConfig.BATCH_SIZE_CONFIG,
                        1024 * 1024);

                props.put(
                        ProducerConfig.LINGER_MS_CONFIG,
                        5);

                props.put(
                        ProducerConfig.BUFFER_MEMORY_CONFIG,
                        64 * 1024 * 1024L);

                props.put(
                        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                        5);

                producer = new KafkaProducer<>(
                        props);

                ObjectMapper mapper = new ObjectMapper();

                Random random = new Random(
                        12345L
                                + workerId);

                long start = System.nanoTime();

                long sent = 0;

                // ------------------------------------------------
                // Event generation
                // ------------------------------------------------

                for (long i = 0; i < numEvents; i++) {

                    long eventId = startEventId + i;

                    // --------------------------------------------
                    // Entity
                    // --------------------------------------------

                    String entityId = entityPool.next(
                            random);

                    // --------------------------------------------
                    // Event
                    // --------------------------------------------

                    Event event = generateEvent(
                            eventId,
                            entityId,
                            random,
                            config);

                    byte[] json = mapper.writeValueAsBytes(
                            event);

                    // --------------------------------------------
                    // Kafka key
                    // --------------------------------------------

                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                            config.topic,
                            entityId,
                            json);

                    producer.send(record);

                    sent++;
                }

                // ------------------------------------------------
                // Flush
                // ------------------------------------------------

                producer.flush();

                double elapsed = (System.nanoTime()
                        - start)
                        / 1_000_000_000.0;

                double throughput = sent / elapsed;

                totalSent.addAndGet(sent);

                System.out.printf(
                        "Worker %d: %,d events in %.2fs (%,.0f events/s)%n",
                        workerId,
                        sent,
                        elapsed,
                        throughput);

            } catch (Exception e) {

                System.err.printf(
                        "Worker %d failed: %s%n",
                        workerId,
                        e.getMessage());

                e.printStackTrace();

            } finally {

                if (producer != null) {

                    producer.close();
                }

                latch.countDown();
            }
        }
    }

    // ============================================================
    // Generate Event
    // ============================================================

    static Event generateEvent(
            long eventId,
            String entityId,
            Random random,
            Config config) {

        String eventType = EVENT_TYPES[random.nextInt(
                EVENT_TYPES.length)];

        Instant eventTime = Instant.now()
                .minus(
                        random.nextInt(
                                24 * 60),
                        ChronoUnit.MINUTES);

        // --------------------------------------------------------
        // Late event
        // --------------------------------------------------------

        if (random.nextDouble() < config.lateEventRate) {

            eventTime = eventTime.minus(
                    random.nextInt(
                            120),
                    ChronoUnit.MINUTES);
        }

        Instant ingestionTime = Instant.now();

        // --------------------------------------------------------
        // Attributes
        // --------------------------------------------------------

        Map<String, Object> attributes = new HashMap<>(
                config.numAttributes);

        attributes.put(
                "product_id",
                "product-"
                        + random.nextInt(
                                10_000));

        attributes.put(
                "category",
                CATEGORIES[random.nextInt(
                        CATEGORIES.length)]);

        attributes.put(
                "city",
                CITIES[random.nextInt(
                        CITIES.length)]);

        attributes.put(
                "device",
                DEVICES[random.nextInt(
                        DEVICES.length)]);

        if (eventType.equals(
                "purchase")) {

            attributes.put(
                    "amount",
                    10_000
                            + random.nextDouble()
                                    * 20_000_000);

            attributes.put(
                    "quantity",
                    1 + random.nextInt(10));

            attributes.put(
                    "payment_method",
                    PAYMENT_METHODS[random.nextInt(
                            PAYMENT_METHODS.length)]);
        }

        if (eventType.equals(
                "view_product")) {

            attributes.put(
                    "duration_seconds",
                    1 + random.nextInt(600));

            attributes.put(
                    "source",
                    SOURCES[random.nextInt(
                            SOURCES.length)]);
        }

        if (eventType.equals("login")) {

            attributes.put(
                    "login_method",
                    LOGIN_METHODS[random.nextInt(
                            LOGIN_METHODS.length)]);
        }

        // --------------------------------------------------------
        // Fill remaining attributes
        // --------------------------------------------------------

        for (int i = attributes.size(); i < config.numAttributes; i++) {

            attributes.put(
                    "field_" + (i + 1),
                    random.nextInt(
                            1_000_000));
        }

        // --------------------------------------------------------
        // Envelope
        // --------------------------------------------------------

        Event event = new Event();

        event.event_id = "evt-" + eventId;

        event.event_type = eventType;

        event.entity_id = entityId;

        event.entity_type = "customer";

        event.event_time = eventTime.toString();

        event.schema_id = "customer-event";

        event.schema_version = "1";

        event.source_system = "ecommerce";

        event.ingestion_time = ingestionTime.toString();

        event.attributes = attributes;

        return event;
    }

    // ============================================================
    // Entity pool
    // ============================================================

    static class EntityPool {

        private final String[] entities;

        private final int hotEntityCount;

        private final double skew;

        EntityPool(
                int numEntities,
                double skew) {

            entities = new String[numEntities];

            for (int i = 0; i < numEntities; i++) {

                entities[i] = "customer-" + i;
            }

            hotEntityCount = Math.max(
                    1,
                    (int) (numEntities
                            * 0.01));

            this.skew = skew;
        }

        String next(
                Random random) {

            if (skew > 0
                    && random.nextDouble() < skew) {

                return entities[random.nextInt(
                        hotEntityCount)];
            }

            return entities[random.nextInt(
                    entities.length)];
        }

        int size() {

            return entities.length;
        }
    }

    // ============================================================
    // Event schema
    // ============================================================

    public static class Event {

        public String event_id;

        public String event_type;

        public String entity_id;

        public String entity_type;

        public String event_time;

        public String schema_id;

        public String schema_version;

        public String source_system;

        public String ingestion_time;

        public Map<String, Object> attributes;
    }

    // ============================================================
    // Config
    // ============================================================

    static class Config {

        String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;

        String topic = DEFAULT_TOPIC;

        long numEvents = DEFAULT_NUM_EVENTS;

        int numAttributes = DEFAULT_NUM_ATTRIBUTES;

        double lateEventRate = DEFAULT_LATE_EVENT_RATE;

        int numEntities = DEFAULT_NUM_ENTITIES;

        double dataSkew = DEFAULT_DATA_SKEW;

        int workers = DEFAULT_WORKERS;

        static Config parse(
                String[] args) {

            Config config = new Config();

            for (int i = 0; i < args.length; i++) {

                switch (args[i]) {

                    case "--bootstrap-servers":
                        config.bootstrapServers = args[++i];
                        break;

                    case "--topic":
                        config.topic = args[++i];
                        break;

                    case "--num-events":
                        config.numEvents = Long.parseLong(
                                args[++i]);
                        break;

                    case "--num-attributes":
                        config.numAttributes = Integer.parseInt(
                                args[++i]);
                        break;

                    case "--late-event-rate":
                        config.lateEventRate = Double.parseDouble(
                                args[++i]);
                        break;

                    case "--num-entities":
                        config.numEntities = Integer.parseInt(
                                args[++i]);
                        break;

                    case "--data-skew":
                        config.dataSkew = Double.parseDouble(
                                args[++i]);
                        break;

                    case "--workers":
                        config.workers = Integer.parseInt(
                                args[++i]);
                        break;

                    default:
                        throw new IllegalArgumentException(
                                "Unknown argument: "
                                        + args[i]);
                }
            }

            return config;
        }
    }

    // ============================================================
    // Validation
    // ============================================================

    static void validate(
            Config config) {

        if (config.numEvents <= 0) {
            throw new IllegalArgumentException(
                    "num-events must be > 0");
        }

        if (config.numAttributes < 1
                || config.numAttributes > 200) {
            throw new IllegalArgumentException(
                    "num-attributes must be 1-200");
        }

        if (config.numEntities <= 0) {
            throw new IllegalArgumentException(
                    "num-entities must be > 0");
        }

        if (config.lateEventRate < 0
                || config.lateEventRate > 1) {
            throw new IllegalArgumentException(
                    "late-event-rate must be 0-1");
        }

        if (config.dataSkew < 0
                || config.dataSkew > 1) {
            throw new IllegalArgumentException(
                    "data-skew must be 0-1");
        }

        if (config.workers <= 0) {
            throw new IllegalArgumentException(
                    "workers must be > 0");
        }
    }
}