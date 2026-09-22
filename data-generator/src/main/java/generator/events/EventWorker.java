package generator.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
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
    private final AtomicLong crmSent;
    private final AtomicLong ecommerceSent;
    private final AtomicLong paymentSent;

    public EventWorker(int workerId, long startEventId, long numEvents, EventConfig config, EntityPool entityPool,
            CountDownLatch latch, AtomicLong totalSent) {
        this(workerId, startEventId, numEvents, config, entityPool, latch, totalSent, null, null, null);
    }

    public EventWorker(int workerId, long startEventId, long numEvents, EventConfig config, EntityPool entityPool,
            CountDownLatch latch, AtomicLong totalSent, AtomicLong crmSent, AtomicLong ecommerceSent,
            AtomicLong paymentSent) {
        this.workerId = workerId;
        this.startEventId = startEventId;
        this.numEvents = numEvents;
        this.config = config;
        this.entityPool = entityPool;
        this.latch = latch;
        this.totalSent = totalSent;
        this.crmSent = crmSent;
        this.ecommerceSent = ecommerceSent;
        this.paymentSent = paymentSent;
    }

    @Override
    public void run() {
        KafkaProducer<String, byte[]> plainProducer = null;
        KafkaProducer<String, byte[]> gssapiProducer = null;

        try {
            boolean isDual = "dual".equalsIgnoreCase(config.cluster) || "multi".equalsIgnoreCase(config.cluster);
            boolean needsPlain = isDual || "plain".equalsIgnoreCase(config.cluster) || "none".equalsIgnoreCase(config.cluster);
            boolean needsGssapi = isDual || "gssapi".equalsIgnoreCase(config.cluster);

            if (needsPlain) {
                plainProducer = new KafkaProducer<>(config.getPlainProducerProperties());
            }
            if (needsGssapi) {
                gssapiProducer = new KafkaProducer<>(config.getGssapiProducerProperties());
            }

            ObjectMapper mapper = new ObjectMapper();
            Random random = new Random(12345L + workerId);
            long start = System.nanoTime();
            long sent = 0;
            long workerCrm = 0;
            long workerEcom = 0;
            long workerPayment = 0;

            for (long i = 0; config.continuous || i < numEvents; i++) {
                long eventId = startEventId + i;
                String entityId = entityPool.next(random);
                Map<String, Object> event = EventFactory.generateEvent(eventId, entityId, random, config);
                byte[] json = mapper.writeValueAsBytes(event);

                String sourceSystem = (String) event.get("source_system");
                String targetTopic;
                if ("crm".equalsIgnoreCase(sourceSystem)) {
                    targetTopic = config.crmTopic;
                } else if ("ecommerce".equalsIgnoreCase(sourceSystem)) {
                    targetTopic = config.ecommerceTopic;
                } else if ("payment".equalsIgnoreCase(sourceSystem)) {
                    targetTopic = config.paymentTopic;
                } else {
                    targetTopic = config.topic + "_" + sourceSystem;
                }
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(targetTopic, entityId, json);

                if (isDual) {
                    if ("crm".equalsIgnoreCase(sourceSystem)) {
                        gssapiProducer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.printf("[Worker %d] Error sending to %s: %s%n", workerId, targetTopic, exception.getMessage());
                            }
                        });
                        workerCrm++;
                    } else if ("ecommerce".equalsIgnoreCase(sourceSystem)) {
                        plainProducer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.printf("[Worker %d] Error sending to %s: %s%n", workerId, targetTopic, exception.getMessage());
                            }
                        });
                        workerEcom++;
                    } else if ("payment".equalsIgnoreCase(sourceSystem)) {
                        plainProducer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.printf("[Worker %d] Error sending to %s: %s%n", workerId, targetTopic, exception.getMessage());
                            }
                        });
                        workerPayment++;
                    } else {
                        plainProducer.send(record);
                    }
                } else if ("gssapi".equalsIgnoreCase(config.cluster)) {
                    gssapiProducer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.printf("[Worker %d] Error sending to %s: %s%n", workerId, targetTopic, exception.getMessage());
                        }
                    });
                    if ("crm".equalsIgnoreCase(sourceSystem)) workerCrm++;
                    else if ("ecommerce".equalsIgnoreCase(sourceSystem)) workerEcom++;
                    else if ("payment".equalsIgnoreCase(sourceSystem)) workerPayment++;
                } else {
                    plainProducer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.printf("[Worker %d] Error sending to %s: %s%n", workerId, targetTopic, exception.getMessage());
                        }
                    });
                    if ("crm".equalsIgnoreCase(sourceSystem)) workerCrm++;
                    else if ("ecommerce".equalsIgnoreCase(sourceSystem)) workerEcom++;
                    else if ("payment".equalsIgnoreCase(sourceSystem)) workerPayment++;
                }
                sent++;

                if (config.continuous && sent % 500_000 == 0) {
                    double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;
                    System.out.printf("Worker %d: sent %,d events (%,.0f events/s)%n", workerId, sent, sent / elapsed);
                }
            }

            if (plainProducer != null) plainProducer.flush();
            if (gssapiProducer != null) gssapiProducer.flush();

            double elapsed = (System.nanoTime() - start) / 1_000_000_000.0;
            totalSent.addAndGet(sent);
            if (crmSent != null) crmSent.addAndGet(workerCrm);
            if (ecommerceSent != null) ecommerceSent.addAndGet(workerEcom);
            if (paymentSent != null) paymentSent.addAndGet(workerPayment);

            System.out.printf("Worker %d: %,d events in %.2fs (%,.0f events/s) [CRM: %,d, Ecom: %,d, Payment: %,d]%n",
                    workerId, sent, elapsed, sent / elapsed, workerCrm, workerEcom, workerPayment);

        } catch (Exception e) {
            System.err.printf("Worker %d failed: %s%n", workerId, e.getMessage());
            e.printStackTrace();
        } finally {
            if (plainProducer != null) {
                try {
                    plainProducer.close();
                } catch (Exception ignored) {
                }
            }
            if (gssapiProducer != null) {
                try {
                    gssapiProducer.close();
                } catch (Exception ignored) {
                }
            }
            latch.countDown();
        }
    }
}
