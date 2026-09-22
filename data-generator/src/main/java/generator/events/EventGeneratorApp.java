package generator.events;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class EventGeneratorApp {

    public static void main(String[] args) throws Exception {
        try {
            ch.qos.logback.classic.Logger kafkaLogger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger("org.apache.kafka");
            kafkaLogger.setLevel(ch.qos.logback.classic.Level.WARN);
        } catch (Throwable ignored) {
        }

        EventConfig config = EventConfig.parse(args);
        config.validate();
        config.initSecurity();

        System.out.println("======================================================================");
        System.out.println("High Throughput Multi-Source Event Generator");
        System.out.println("======================================================================");
        boolean isDual = "dual".equalsIgnoreCase(config.cluster) || "multi".equalsIgnoreCase(config.cluster);
        if (isDual) {
            System.out.printf("Cluster Mode      : DUAL (Multi-Cluster Routing)%n");
            System.out.printf("  -> CRM Events   : %s (Topic: %s, SASL_PLAINTEXT / GSSAPI Kerberos)%n",
                    config.gssapiBootstrapServers, config.crmTopic);
            System.out.printf("  -> Ecom/Payment : %s (Topics: %s, %s, SASL_PLAINTEXT / PLAIN)%n",
                    config.plainBootstrapServers, config.ecommerceTopic, config.paymentTopic);
            System.out.printf("Keytab Path       : %s%n", config.getResolvedKeytab());
            System.out.printf("Kerberos Config   : %s%n", config.getResolvedKrb5Conf());
        } else {
            System.out.printf("Kafka Cluster     : %s%n", config.cluster.toUpperCase());
            System.out.printf("Kafka Bootstrap   : %s%n", config.bootstrapServers);
            System.out.printf("Security Protocol : %s%n", "none".equalsIgnoreCase(config.cluster) ? "PLAINTEXT" : "SASL_PLAINTEXT");
            System.out.printf("SASL Mechanism    : %s%n", "gssapi".equalsIgnoreCase(config.cluster) ? "GSSAPI" : ("none".equalsIgnoreCase(config.cluster) ? "NONE" : "PLAIN"));
            System.out.printf("Topic Prefix      : %s%n", config.topic);
        }
        System.out.printf("Events            : %s%n", config.continuous ? "Continuous" : String.format("%,d", config.numEvents));
        if (config.startTime != null) {
            System.out.printf("Start time        : %s%n", config.startTime);
        }
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
        AtomicLong crmSent = new AtomicLong(0);
        AtomicLong ecommerceSent = new AtomicLong(0);
        AtomicLong paymentSent = new AtomicLong(0);
        Thread[] threads = new Thread[config.workers];
        long currentStartId = 0;
        long globalStart = System.nanoTime();

        for (int workerId = 0; workerId < config.workers; workerId++) {
            long workerEvents = base + (workerId < remainder ? 1 : 0);
            long startId = currentStartId;
            currentStartId += workerEvents;

            EventWorker worker = new EventWorker(workerId, startId, workerEvents, config, entityPool, latch,
                    totalSent, crmSent, ecommerceSent, paymentSent);
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
        System.out.printf("Total events sent  : %,d%n", totalSent.get());
        if (isDual) {
            System.out.printf("  - CRM (GSSAPI)   : %,d -> %s%n", crmSent.get(), config.crmTopic);
            System.out.printf("  - Ecommerce (PLAIN): %,d -> %s%n", ecommerceSent.get(), config.ecommerceTopic);
            System.out.printf("  - Payment (PLAIN): %,d -> %s%n", paymentSent.get(), config.paymentTopic);
        }
        System.out.printf("Elapsed            : %.2f s%n", elapsed);
        System.out.printf("Kafka throughput   : %,.0f events/sec%n", throughput);
        System.out.println("======================================================================");
    }
}
