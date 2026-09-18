package flink;

import flink.config.KafkaClusterConfig;
import flink.operators.DynamicSchemaValidationFunction;
import flink.operators.RingBufferRuleProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class RealtimeCepJob {
    private static final Logger LOG = LoggerFactory.getLogger(RealtimeCepJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Real-time CEP Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        int parallelism = parameters.getInt("parallelism", 4);
        env.setParallelism(parallelism);
        env.getConfig().setLatencyTrackingInterval(parameters.getLong("latency.tracking.interval", 5000));

        String schemaTopic = parameters.get("schema.topic", "schema_registry");
        String ruleTopic = parameters.get("rule.topic", "rule_definitions");
        String eventsTopicPattern = parameters.get("events.topic.pattern", "events_.*");
        String resultTopic = parameters.get("result.topic", "result");
        String dlqTopic = parameters.get("dlq.topic", "dlq");

        if (parameters.has("checkpoint.dir")) {
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, parameters.get("checkpoint.dir"));
            env.configure(config);
        }

        // Cấu hình kết nối cho từng cụm Kafka (mặc định schema từ GSSAPI, còn lại từ PLAIN)
        String schemaBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "schema", KafkaClusterConfig.CLUSTER_GSSAPI);
        Properties schemaProps = KafkaClusterConfig.getConsumerProperties(parameters, "schema", KafkaClusterConfig.CLUSTER_GSSAPI);

        String ruleBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "rule", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties ruleProps = KafkaClusterConfig.getConsumerProperties(parameters, "rule", KafkaClusterConfig.CLUSTER_PLAIN);

        String eventsBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "events", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties eventsProps = KafkaClusterConfig.getConsumerProperties(parameters, "events", KafkaClusterConfig.CLUSTER_PLAIN);

        String resultBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties resultProps = KafkaClusterConfig.getProducerProperties(parameters, "result", KafkaClusterConfig.CLUSTER_PLAIN);

        String dlqBootstrap = KafkaClusterConfig.getBootstrapServers(parameters, "dlq", KafkaClusterConfig.CLUSTER_PLAIN);
        Properties dlqProps = KafkaClusterConfig.getProducerProperties(parameters, "dlq", KafkaClusterConfig.CLUSTER_PLAIN);

        LOG.info("Schema Source -> Bootstrap: {}, Topic: {}", schemaBootstrap, schemaTopic);
        LOG.info("Rule Source -> Bootstrap: {}, Topic: {}", ruleBootstrap, ruleTopic);
        LOG.info("Events Source -> Bootstrap: {}, Topic Pattern: {}", eventsBootstrap, eventsTopicPattern);
        LOG.info("Result Sink -> Bootstrap: {}, Topic: {}", resultBootstrap, resultTopic);
        LOG.info("DLQ Sink -> Bootstrap: {}, Topic: {}", dlqBootstrap, dlqTopic);

        KafkaSource<String> schemaSource = KafkaSource.<String>builder()
                .setBootstrapServers(schemaBootstrap)
                .setTopics(schemaTopic)
                .setGroupId("flink-schema-group")
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

        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers(ruleBootstrap)
                .setTopics(ruleTopic)
                .setGroupId("flink-rule-group")
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

        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(eventsBootstrap)
                .setTopicPattern(java.util.regex.Pattern.compile(eventsTopicPattern))
                .setGroupId("flink-event-validation-group")
                .setProperty("partition.discovery.interval.ms", "60000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(eventsProps)
                .build();

        DataStream<String> eventStream = env.fromSource(
                eventSource,
                eventWatermarkStrategy,
                "Events Multi-Topic Source");

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

