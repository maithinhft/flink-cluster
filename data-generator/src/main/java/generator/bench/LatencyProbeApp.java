package generator.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import generator.common.ClientConfig;
import generator.events.EventConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Measures end-to-end alert latency from the {@code result} topic for a fixed duration.
 * latency = emit_time_ms (set by the engine) - ingest_time_ms (Kafka timestamp of the triggering event);
 * also reports the latency up to the result record's own Kafka timestamp (includes sink batching).
 *
 * <pre>
 * --duration-s 300   --from latest|earliest   --csv bench/results.csv   --label S1-10k
 * </pre>
 */
public class LatencyProbeApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String topic = ClientConfig.arg(args, "--result-topic", "result");
        long durationMs = Long.parseLong(ClientConfig.arg(args, "--duration-s", "300")) * 1000L;
        boolean fromEarliest = "earliest".equalsIgnoreCase(ClientConfig.arg(args, "--from", "latest"));
        String csv = ClientConfig.arg(args, "--csv", null);
        String label = ClientConfig.arg(args, "--label", "run");

        EventConfig cfg = ClientConfig.eventConfig(args);
        Properties props = ClientConfig.consumer(cfg, false, null);
        long[] engine = new long[1 << 16];
        long[] toKafka = new long[1 << 16];
        int n = 0;
        long first = Long.MAX_VALUE;
        long last = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> parts = new ArrayList<>();
            for (PartitionInfo pi : consumer.partitionsFor(topic)) {
                parts.add(new TopicPartition(topic, pi.partition()));
            }
            consumer.assign(parts);
            if (fromEarliest) {
                consumer.seekToBeginning(parts);
            } else {
                consumer.seekToEnd(parts);
            }
            long deadline = System.currentTimeMillis() + durationMs;
            System.out.printf("Probing %s for %d s ...%n", topic, durationMs / 1000);
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    JsonNode lat = MAPPER.readTree(r.value()).path("latency");
                    long ingest = lat.path("ingest_time_ms").asLong(0);
                    long emit = lat.path("emit_time_ms").asLong(0);
                    if (ingest <= 0 || emit <= 0) {
                        continue;
                    }
                    if (n == engine.length) {
                        engine = Arrays.copyOf(engine, n * 2);
                        toKafka = Arrays.copyOf(toKafka, n * 2);
                    }
                    engine[n] = Math.max(0, emit - ingest);
                    toKafka[n] = Math.max(0, r.timestamp() - ingest);
                    n++;
                    first = Math.min(first, r.timestamp());
                    last = Math.max(last, r.timestamp());
                }
            }
        }
        if (n == 0) {
            System.out.println("No alerts observed.");
            return;
        }
        long[] e = Arrays.copyOf(engine, n);
        long[] k = Arrays.copyOf(toKafka, n);
        Arrays.sort(e);
        Arrays.sort(k);
        double rate = n / Math.max(1.0, (last - first) / 1000.0);
        System.out.printf("alerts=%,d rate=%.1f/s%n", n, rate);
        System.out.printf("engine  latency ms: p50=%d p95=%d p99=%d max=%d%n", pct(e, 50), pct(e, 95), pct(e, 99), e[n - 1]);
        System.out.printf("to-topic latency ms: p50=%d p95=%d p99=%d max=%d%n", pct(k, 50), pct(k, 95), pct(k, 99), k[n - 1]);
        if (csv != null) {
            appendCsv(csv, label, n, rate, e, k);
        }
    }

    static long pct(long[] sorted, double p) {
        int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }

    private static void appendCsv(String path, String label, int n, double rate, long[] e, long[] k) throws IOException {
        boolean header = !Files.exists(Paths.get(path));
        if (Paths.get(path).getParent() != null) {
            Files.createDirectories(Paths.get(path).getParent());
        }
        try (FileWriter w = new FileWriter(path, true)) {
            if (header) {
                w.write("label,alerts,alerts_per_s,engine_p50_ms,engine_p95_ms,engine_p99_ms,topic_p50_ms,topic_p95_ms,topic_p99_ms\n");
            }
            w.write(String.format("%s,%d,%.1f,%d,%d,%d,%d,%d,%d%n", label, n, rate,
                    pct(e, 50), pct(e, 95), pct(e, 99), pct(k, 50), pct(k, 95), pct(k, 99)));
        }
    }
}
