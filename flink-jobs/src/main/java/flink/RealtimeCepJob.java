package flink;

import flink.config.KafkaClusterConfig;
import flink.dynamic.metadata.PostgresKafkaMetadataService;
import flink.operators.DynamicSchemaValidationFunction;
import flink.operators.RingBufferRuleProcessFunction;
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
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class RealtimeCepJob {
    private static final Logger LOG = LoggerFactory.getLogger(RealtimeCepJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Real-time CEP Job with DynamicKafkaSource...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        int parallelism = parameters.getInt("parallelism", 4);
        env.setParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(parameters.getLong("latency.tracking.interval", 5000L));

        if (parameters.has("checkpoint.dir")) {
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, parameters.get("checkpoint.dir"));
            env.configure(config);
        }

        // 1. Cấu hình Schema Source (KafkaSource đọc trực tiếp từ cụm GSSAPI / Kerberos)
        String schemaBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "schema", KafkaClusterConfig.CLUSTER_GSSAPI);
        Properties schemaProps = KafkaClusterConfig.getConsumerProperties(parameters, "schema", KafkaClusterConfig.CLUSTER_GSSAPI);
        String schemaTopic = parameters.get("schema.topic", "schema_registry");
        LOG.info("Schema Source (GSSAPI) -> Bootstrap: {}, Topic: {}", schemaBootstrap, schemaTopic);

        KafkaSource<String> schemaSource = KafkaSource.<String>builder()
                .setBootstrapServers(schemaBootstrap)
                .setTopics(schemaTopic)
                .setGroupId(parameters.get("schema.group.id", "flink-schema-group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(schemaProps)
                .build();

        DataStream<String> schemaStream = env.fromSource(
                schemaSource,
                WatermarkStrategy.noWatermarks(),
                "Schema Registry Source");

        BroadcastStream<String> broadcastSchemaStream = schemaStream
                .broadcast(
                        DynamicSchemaValidationFunction.SCHEMA_STATE_DESCRIPTOR,
                        DynamicSchemaValidationFunction.LATEST_VERSION_DESCRIPTOR,
                        DynamicSchemaValidationFunction.DEPRECATED_SCHEMAS_DESCRIPTOR);

        // 2. Cấu hình Rule Source (KafkaSource đọc trực tiếp từ cụm PLAIN)
        String ruleBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties ruleProps = KafkaClusterConfig.getConsumerProperties(parameters, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
        String ruleTopic = parameters.get("rule.topic", "rule_definitions");
        LOG.info("Rule Source (PLAIN) -> Bootstrap: {}, Topic: {}", ruleBootstrap, ruleTopic);

        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers(ruleBootstrap)
                .setTopics(ruleTopic)
                .setGroupId(parameters.get("rule.group.id", "flink-rule-group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(ruleProps)
                .build();

        DataStream<String> ruleStream = env.fromSource(
                ruleSource,
                WatermarkStrategy.noWatermarks(),
                "Rule Definitions Source");

        BroadcastStream<String> broadcastRuleStream = ruleStream
                .broadcast(RingBufferRuleProcessFunction.RULE_STATE_DESCRIPTOR);

        // 3. Cấu hình PostgreSQL Metadata Service cho Event Stream (DynamicKafkaSource)
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

        // 4. Cấu hình Result & DLQ Sink (cụm PLAIN)
        String resultBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties resultProps = KafkaClusterConfig.getProducerProperties(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        String resultTopic = parameters.get("result.topic", "result");

        String dlqBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "dlq", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties dlqProps = KafkaClusterConfig.getProducerProperties(parameters, "dlq", KafkaClusterConfig.CLUSTER_PLAIN);
        String dlqTopic = parameters.get("dlq.topic", "dlq");

        LOG.info("Result Sink -> Bootstrap: {}, Topic: {}", resultBootstrap, resultTopic);
        LOG.info("DLQ Sink -> Bootstrap: {}, Topic: {}", dlqBootstrap, dlqTopic);

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

        // 5. Khởi tạo DynamicKafkaSource cho Event Multi-Cluster Source
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
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setGroupId(parameters.get("events.group.id", "flink-event-validation-group"))
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
                    .setGroupId(parameters.get("events.group.id", "flink-event-validation-group"))
                    .setProperty("partition.discovery.interval.ms", "60000")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperties(eventsProps)
                    .build();

            eventStream = env.fromSource(
                    staticEventSource,
                    eventWatermarkStrategy,
                    "Events Multi-Topic Source");
        }

        SingleOutputStreamOperator<String> cleanEventsStream = eventStream
                .connect(broadcastSchemaStream)
                .process(new DynamicSchemaValidationFunction())
                .name("Dynamic Schema Validation Operator");

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

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(dlqBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(dlqTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(dlqProps)
                .build();

        DataStream<String> dirtyEventsStream = cleanEventsStream
                .getSideOutput(DynamicSchemaValidationFunction.DIRTY_DATA_TAG);

        SingleOutputStreamOperator<String> aggregatedStream = cleanEventsStream
                .keyBy(eventJson -> {
                    try {
                        int idx = eventJson.indexOf("\"entity_id\"");
                        if (idx != -1) {
                            int startQuote = eventJson.indexOf('"', idx + 11);
                            int endQuote = eventJson.indexOf('"', startQuote + 1);
                            if (startQuote != -1 && endQuote != -1) {
                                return eventJson.substring(startQuote + 1, endQuote);
                            }
                        }
                    } catch (Exception ignored) {
                    }
                    return "unknown_entity";
                })
                .connect(broadcastRuleStream)
                .process(new RingBufferRuleProcessFunction())
                .name("Ring Buffer & Rule Evaluation Operator")
                .disableChaining();

        DataStream<String> lateEventsStream = aggregatedStream
                .getSideOutput(RingBufferRuleProcessFunction.LATE_DATA_TAG);

        DataStream<String> dlqStream = dirtyEventsStream.union(lateEventsStream);
        dlqStream.sinkTo(dlqSink).name("DLQ Kafka Sink");

        aggregatedStream.sinkTo(resultSink).name("Aggregated Result Kafka Sink");

        env.execute("Flink Real-time CEP & Rule Engine Job");
    }
}

