package flink;

import flink.config.KafkaClusterConfig;
import flink.dynamic.metadata.PostgresKafkaMetadataService;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * Secondary Flink Job: Dynamic Kafka Pass-Through Job.
 * Ingests events from multi-cluster Kafka using DynamicKafkaSource (PostgreSQL catalog)
 * and directly forwards all raw event records to the 'result' topic without additional processing.
 */
public class DynamicPassThroughJob {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPassThroughJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Dynamic Kafka Pass-Through Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        int parallelism = parameters.getInt("parallelism", 4);
        env.setParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(parameters.getLong("latency.tracking.interval", 5000L));

        // Cấu hình Checkpointing: mặc định 5 phút / lần (300,000 ms)
        long checkpointInterval = parameters.getLong("checkpoint.interval", 300_000L);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(parameters.getLong("checkpoint.min.pause", 30_000L)); // Nghỉ 30s giữa các lần checkpoint
        checkpointConfig.setCheckpointTimeout(parameters.getLong("checkpoint.timeout", 120_000L)); // Timeout 2 phút
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        if (parameters.has("checkpoint.dir")) {
            checkpointConfig.setCheckpointStorage(parameters.get("checkpoint.dir"));
        }

        // 1. Cấu hình PostgreSQL Metadata Service cho Event Stream (DynamicKafkaSource)
        String pgHost = parameters.get("postgres.host", "postgres");
        String pgPort = parameters.get("postgres.port", "5432");
        String pgDb = parameters.get("postgres.db", "realtime_core");
        String defaultPgUrl = String.format("jdbc:postgresql://%s:%s/%s", pgHost, pgPort, pgDb);
        String pgUrl = parameters.get("postgres.url", defaultPgUrl);
        String pgUser = parameters.get("postgres.user", "postgres");
        String pgPassword = parameters.get("postgres.password", "postgres");
        String tablePrefix = parameters.get("postgres.table.prefix", "kafka_stream");
        long discoveryIntervalMs = parameters.getLong("stream.metadata.discovery.interval.ms", 30000L);

        LOG.info("Connecting to PostgreSQL metadata: {} (tablePrefix: {})", pgUrl, tablePrefix);
        PostgresKafkaMetadataService metadataService = new PostgresKafkaMetadataService(
                pgUrl, pgUser, pgPassword, tablePrefix, 5000L);

        // 2. Cấu hình Result Sink (cụm PLAIN)
        String resultBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties resultProps = KafkaClusterConfig.getProducerProperties(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        String resultTopic = parameters.get("result.topic", "result");
        LOG.info("Result Sink -> Bootstrap: {}, Topic: {}", resultBootstrap, resultTopic);

        KafkaSink<String> resultSink = KafkaSink.<String>builder()
                .setBootstrapServers(resultBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(resultTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(resultProps)
                .build();

        // 3. Watermark Strategy (tương tự RealtimeCepJob, hỗ trợ theo dõi Watermarks)
        WatermarkStrategy<String> eventWatermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((eventJson, recordTimestamp) -> {
                    try {
                        int idx = eventJson.indexOf("\"event_time\"");
                        if (idx != -1) {
                            int startQuote = eventJson.indexOf('"', idx + 12);
                            int endQuote = eventJson.indexOf('"', startQuote + 1);
                            if (startQuote != -1 && endQuote != -1) {
                                String timeStr = eventJson.substring(startQuote + 1, endQuote);
                                return Instant.parse(timeStr).toEpochMilli();
                            }
                        }
                    } catch (Exception ignored) {
                    }
                    return recordTimestamp > 0 ? recordTimestamp : System.currentTimeMillis();
                })
                .withIdleness(Duration.ofMinutes(1));

        // 4. Khởi tạo DynamicKafkaSource cho Event Multi-Cluster Source
        String eventsStreamId = parameters.get("events.stream.id", "stream-events");
        boolean useDynamicEventSource = parameters.getBoolean("use.dynamic.source", true);

        DataStream<String> eventStream;
        if (useDynamicEventSource) {
            LOG.info("Creating DynamicKafkaSource for events subscribing to stream '{}' via PostgreSQL (discovery interval: {} ms)",
                    eventsStreamId, discoveryIntervalMs);

            DynamicKafkaSource<String> dynamicEventSource = DynamicKafkaSource.<String>builder()
                    .setKafkaMetadataService(metadataService)
                    .setStreamIds(Collections.singleton(eventsStreamId))
                    .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                    .setGroupId(parameters.get("events.group.id", "flink-passthrough-group"))
                    .setProperty(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(), String.valueOf(discoveryIntervalMs))
                    .build();

            eventStream = env.fromSource(
                    dynamicEventSource,
                    eventWatermarkStrategy,
                    "Dynamic Events Multi-Cluster Source");
        } else {
            String eventsBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "events", KafkaClusterConfig.CLUSTER_PLAIN);
            Properties eventsProps = KafkaClusterConfig.getConsumerProperties(parameters, "events", KafkaClusterConfig.CLUSTER_PLAIN);
            KafkaSource<String> staticEventSource = KafkaSource.<String>builder()
                    .setBootstrapServers(eventsBootstrap)
                    .setTopicPattern(java.util.regex.Pattern.compile(parameters.get("events.topic.pattern", "events_.*")))
                    .setGroupId(parameters.get("events.group.id", "flink-passthrough-group"))
                    .setProperty("partition.discovery.interval.ms", "60000")
                    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperties(eventsProps)
                    .build();

            eventStream = env.fromSource(
                    staticEventSource,
                    eventWatermarkStrategy,
                    "Events Multi-Topic Source");
        }

        // 5. Đẩy thẳng trực tiếp ra Result Sink (không xử lý gì thêm)
        eventStream.sinkTo(resultSink).name("Result Kafka Sink");

        env.execute("Flink Dynamic Kafka Pass-Through Job");
    }
}

