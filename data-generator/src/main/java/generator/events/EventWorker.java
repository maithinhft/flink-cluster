package generator.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class EventWorker implements Runnable {
    private final int workerId;
    private final long startEventId;
    private final long numEvents;
    private final EventConfig config;
    private final EntityPool entityPool;
    private final CountDownLatch latch;
    private final AtomicLong totalSent;

    public EventWorker(int workerId, long startEventId, long numEvents, EventConfig config, EntityPool entityPool,
            CountDownLatch latch, AtomicLong totalSent) {
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
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.ByteArraySerializer");
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
                Map<String, Object> event = EventFactory.generateEvent(eventId, entityId, random, config);
                byte[] json = mapper.writeValueAsBytes(event);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(config.topic, entityId, json);
                producer.send(record);
                sent++;
            }

            producer.flush();
            double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;
            totalSent.addAndGet(sent);
            System.out.printf("Worker %d: %,d events in %.2fs (%,.0f events/s)%n", workerId, sent, elapsed,
                    sent / elapsed);

        } catch (Exception e) {
            System.err.printf("Worker %d failed: %s%n", workerId, e.getMessage());
            e.printStackTrace();
        } finally {
            if (producer != null)
                producer.close();
            latch.countDown();
        }
    }
}
