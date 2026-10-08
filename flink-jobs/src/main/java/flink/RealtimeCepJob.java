package flink;

import flink.config.KafkaClusterConfig;
import flink.control.ControlMessageParser;
import flink.dynamic.metadata.PostgresKafkaMetadataService;
import flink.engine.EngineConfig;
import flink.io.KafkaIo;
import flink.model.ControlMessage;
import flink.model.KeyedRecord;
import flink.model.RawEvent;
import flink.model.ValidatedEvent;
import flink.operators.DynamicSchemaValidationFunction;
import flink.operators.RingBufferRuleProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSource;
import org.apache.flink.connector.kafka.dynamic.source.DynamicKafkaSourceOptions;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class RealtimeCepJob {
    private static final Logger LOG = LoggerFactory.getLogger(RealtimeCepJob.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureEnvironment(env, params);
        build(env, params);
        env.execute(params.get("job.name", "Flink Real-time CEP & Rule Engine Job"));
    }

    static void configureEnvironment(StreamExecutionEnvironment env, ParameterTool params) {
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 4));
        env.setMaxParallelism(params.getInt("max.parallelism", 128));
        long latencyTracking = params.getLong("latency.tracking.interval", 0L);
        if (latencyTracking > 0) {
            env.getConfig().setLatencyTrackingInterval(latencyTracking);
        }
        env.setBufferTimeout(params.getLong("buffer.timeout.ms", 10L));
        if (params.getBoolean("object.reuse", true)) {
            env.getConfig().enableObjectReuse();
        }
        if (params.has("checkpoint.dir")) {
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint.dir"));
            env.configure(config);
        }
        if (params.has("checkpoint.interval.ms")) {
            env.enableCheckpointing(params.getLong("checkpoint.interval.ms"), CheckpointingMode.EXACTLY_ONCE);
            CheckpointConfig cc = env.getCheckpointConfig();
            cc.setMinPauseBetweenCheckpoints(params.getLong("checkpoint.min.pause.ms", 5000L));
            cc.setCheckpointTimeout(params.getLong("checkpoint.timeout.ms", 300000L));
            cc.setTolerableCheckpointFailureNumber(params.getInt("checkpoint.tolerable.failures", 3));
            cc.setMaxConcurrentCheckpoints(1);
            cc.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
            cc.enableUnalignedCheckpoints(params.getBoolean("checkpoint.unaligned", true));
            cc.setAlignedCheckpointTimeout(Duration.ofMillis(params.getLong("checkpoint.aligned.timeout.ms", 5000L)));
        }
    }

    static void build(StreamExecutionEnvironment env, ParameterTool params) {
        EngineConfig engineConfig = EngineConfig.fromParameters(params);

        DataStream<ControlMessage> control = controlStream(env, params);
        BroadcastStream<ControlMessage> controlForValidation =
                control.broadcast(DynamicSchemaValidationFunction.descriptors());
        BroadcastStream<ControlMessage> controlForEngine =
                control.broadcast(RingBufferRuleProcessFunction.descriptors());

        DataStream<RawEvent> events = eventStream(env, params);

        SingleOutputStreamOperator<ValidatedEvent> validated = events
                .connect(controlForValidation)
                .process(new DynamicSchemaValidationFunction(engineConfig))
                .name("Validate & Project")
                .uid("op-validate");

        SingleOutputStreamOperator<KeyedRecord> alerts = validated
                .keyBy(ev -> ev.entityId, Types.STRING)
                .connect(controlForEngine)
                .process(new RingBufferRuleProcessFunction(engineConfig))
                .name("Segment Engine")
                .uid("op-engine");

        alerts.sinkTo(resultSink(params)).name("Result Kafka Sink").uid("sink-result");

        validated.getSideOutput(DynamicSchemaValidationFunction.DIRTY_DATA_TAG)
                .union(alerts.getSideOutput(RingBufferRuleProcessFunction.LATE_DATA_TAG))
                .sinkTo(dlqSink(params))
                .name("DLQ Kafka Sink")
                .uid("sink-dlq");

        LOG.info("Engine configured: index={}, effectiveMode={}, projection={}, bootstrap={}",
                engineConfig.indexMode, engineConfig.effectiveMode, engineConfig.projectionEnabled,
                engineConfig.bootstrapEnabled);
    }

    static DataStream<ControlMessage> controlStream(StreamExecutionEnvironment env, ParameterTool params) {
        KafkaSource<String> schemaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaClusterConfig.getBootstrapServers(params, "schema", KafkaClusterConfig.CLUSTER_GSSAPI))
                .setProperties(KafkaClusterConfig.getConsumerProperties(params, "schema", KafkaClusterConfig.CLUSTER_GSSAPI))
                .setTopics(params.get("schema.topic", "schema_registry"))
                .setGroupId(params.get("schema.group.id", "flink-schema-group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaIo.NullSafeStringDeserializer())
                .build();

        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaClusterConfig.getBootstrapServers(params, "rule", KafkaClusterConfig.CLUSTER_PLAIN))
                .setProperties(KafkaClusterConfig.getConsumerProperties(params, "rule", KafkaClusterConfig.CLUSTER_PLAIN))
                .setTopics(params.get("rule.topic", "rule_definitions"))
                .setGroupId(params.get("rule.group.id", "flink-rule-group"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new KafkaIo.NullSafeStringDeserializer())
                .build();

        DataStream<String> schemaRaw = env
                .fromSource(schemaSource, KafkaIo.<String>controlWatermarks(), "Schema Registry Source")
                .setParallelism(1)
                .uid("src-control-schema");

        DataStream<String> ruleRaw = env
                .fromSource(ruleSource, KafkaIo.<String>controlWatermarks(), "Rule Definitions Source")
                .setParallelism(1)
                .uid("src-control-rule");

        return schemaRaw.union(ruleRaw)
                .flatMap(new ControlMessageParser())
                .setParallelism(1)
                .name("Control Parser")
                .uid("map-control");
    }

    static DataStream<RawEvent> eventStream(StreamExecutionEnvironment env, ParameterTool params) {
        WatermarkStrategy<RawEvent> watermarks = KafkaIo.eventWatermarks(
                Duration.ofMillis(params.getLong("watermark.out.of.orderness.ms", 60000L)),
                Duration.ofMillis(params.getLong("watermark.idleness.ms", 60000L)),
                params.getLong("watermark.alignment.max.drift.ms", 0L));
        OffsetsInitializer offsets = eventOffsets(params.get("events.starting.offsets", "committed"));
        String groupId = params.get("events.group.id", "flink-event-validation-group");

        if (params.getBoolean("use.dynamic.source", true)) {
            EngineConfig pg = EngineConfig.fromParameters(params);
            PostgresKafkaMetadataService metadata = new PostgresKafkaMetadataService(
                    pg.bootstrapUrl, pg.bootstrapUser, pg.bootstrapPassword,
                    params.get("postgres.table.prefix", "kafka_stream"), 5000L);
            DynamicKafkaSource<RawEvent> source = DynamicKafkaSource.<RawEvent>builder()
                    .setKafkaMetadataService(metadata)
                    .setStreamIds(Collections.singleton(params.get("events.stream.id", "stream-events")))
                    .setDeserializer(new KafkaIo.RawEventDeserializer())
                    .setStartingOffsets(offsets)
                    .setGroupId(groupId)
                    .setProperty(DynamicKafkaSourceOptions.STREAM_METADATA_DISCOVERY_INTERVAL_MS.key(),
                            String.valueOf(params.getLong("stream.metadata.discovery.interval.ms", 30000L)))
                    .setProperty("commit.offsets.on.checkpoint", "true")
                    .build();
            return env.fromSource(source, watermarks, "Dynamic Events Multi-Cluster Source").uid("src-events");
        }

        KafkaSource<RawEvent> source = KafkaSource.<RawEvent>builder()
                .setBootstrapServers(KafkaClusterConfig.getBootstrapServers(params, "events", KafkaClusterConfig.CLUSTER_PLAIN))
                .setProperties(KafkaClusterConfig.getConsumerProperties(params, "events", KafkaClusterConfig.CLUSTER_PLAIN))
                .setTopicPattern(Pattern.compile(params.get("events.topic.pattern", "events_.*")))
                .setGroupId(groupId)
                .setProperty("partition.discovery.interval.ms", "60000")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setStartingOffsets(offsets)
                .setDeserializer(new KafkaIo.RawEventDeserializer())
                .build();
        return env.fromSource(source, watermarks, "Events Multi-Topic Source").uid("src-events");
    }

    static OffsetsInitializer eventOffsets(String mode) {
        switch (mode.trim().toLowerCase()) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            default:
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        }
    }

    static Properties producerTuning(Properties base, ParameterTool params) {
        Properties p = new Properties();
        p.putAll(base);
        p.setProperty("linger.ms", params.get("producer.linger.ms", "20"));
        p.setProperty("batch.size", params.get("producer.batch.size", "131072"));
        p.setProperty("compression.type", params.get("producer.compression", "lz4"));
        p.setProperty("acks", "all");
        p.setProperty("enable.idempotence", "true");
        p.setProperty("delivery.timeout.ms", params.get("producer.delivery.timeout.ms", "120000"));
        return p;
    }

    static KafkaSink<KeyedRecord> resultSink(ParameterTool params) {
        Properties props = producerTuning(
                KafkaClusterConfig.getProducerProperties(params, "result", KafkaClusterConfig.CLUSTER_PLAIN), params);
        KafkaSinkBuilder<KeyedRecord> b = KafkaSink.<KeyedRecord>builder()
                .setBootstrapServers(KafkaClusterConfig.getBootstrapServers(params, "result", KafkaClusterConfig.CLUSTER_PLAIN))
                .setRecordSerializer(new KafkaIo.KeyedRecordSerializer(params.get("result.topic", "result")));
        if (params.getBoolean("result.exactly.once", false)) {
            props.setProperty("transaction.timeout.ms", params.get("result.transaction.timeout.ms", "900000"));
            b.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setTransactionalIdPrefix(params.get("result.transactional.id.prefix", "cep-result"));
        } else {
            b.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        }
        return b.setKafkaProducerConfig(props).build();
    }

    static KafkaSink<KeyedRecord> dlqSink(ParameterTool params) {
        Properties props = producerTuning(
                KafkaClusterConfig.getProducerProperties(params, "dlq", KafkaClusterConfig.CLUSTER_PLAIN), params);
        return KafkaSink.<KeyedRecord>builder()
                .setBootstrapServers(KafkaClusterConfig.getBootstrapServers(params, "dlq", KafkaClusterConfig.CLUSTER_PLAIN))
                .setRecordSerializer(new KafkaIo.KeyedRecordSerializer(params.get("dlq.topic", "dlq")))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(props)
                .build();
    }
}
