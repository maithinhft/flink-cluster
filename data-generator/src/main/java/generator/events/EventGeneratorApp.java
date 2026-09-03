package generator.events;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class EventGeneratorApp {

    public static void main(String[] args) throws Exception {
        EventConfig config = EventConfig.parse(args);
        config.validate();

        System.out.println("======================================================================");
        System.out.println("High Throughput Multi-Source Event Generator");
        System.out.println("======================================================================");
        System.out.printf("Kafka             : %s%n", config.bootstrapServers);
        System.out.printf("Topic             : %s%n", config.topic);
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
        Thread[] threads = new Thread[config.workers];
        long currentStartId = 0;
        long globalStart = System.nanoTime();

        for (int workerId = 0; workerId < config.workers; workerId++) {
            long workerEvents = base + (workerId < remainder ? 1 : 0);
            long startId = currentStartId;
            currentStartId += workerEvents;

            EventWorker worker = new EventWorker(workerId, startId, workerEvents, config, entityPool, latch, totalSent);
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
}
