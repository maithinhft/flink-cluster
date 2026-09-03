package flink;

import flink.operators.SchemaValidationBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationJob {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Schema Validation Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(parameters);

        String bootstrapServers = parameters.get("bootstrap.servers", "kafka:29092");

        String eventsTopicPattern = parameters.get("events.topic.pattern", "events.*");
        String schemaTopic = parameters.get("schema.topic", "schema_registry");

        LOG.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        LOG.info("Events Topic Pattern: {}", eventsTopicPattern);
        LOG.info("Schema Topic: {}", schemaTopic);

        KafkaSource<String> schemaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(schemaTopic)
                .setGroupId("flink-schema-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> schemaStream = env.fromSource(
                schemaSource,
                WatermarkStrategy.noWatermarks(),
                "Schema Registry Source");

        BroadcastStream<String> broadcastSchemaStream = schemaStream
                .broadcast(
                        SchemaValidationBroadcastProcessFunction.SCHEMA_STATE_DESCRIPTOR,
                        SchemaValidationBroadcastProcessFunction.LATEST_VERSION_DESCRIPTOR,
                        SchemaValidationBroadcastProcessFunction.DEPRECATED_SCHEMAS_DESCRIPTOR);

        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(java.util.regex.Pattern.compile(eventsTopicPattern))
                .setGroupId("flink-event-validation-group")
                .setProperty("partition.discovery.interval.ms", "60000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> eventStream = env.fromSource(
                eventSource,
                WatermarkStrategy.noWatermarks(),
                "Events Source");

        SingleOutputStreamOperator<String> processedStream = eventStream
                .connect(broadcastSchemaStream)
                .process(new SchemaValidationBroadcastProcessFunction())
                .name("Schema Validation Operator");

        DataStream<String> dirtyEventsStream = processedStream
                .getSideOutput(SchemaValidationBroadcastProcessFunction.DIRTY_DATA_TAG);
        dirtyEventsStream.map(data -> {
            LOG.info("DIRTY DATA -> {}", data);
            return data;
        });

        processedStream.map(data -> {
            LOG.info("VALID DATA -> {}", data);
            return data;
        });

        env.execute("Flink Realtime Schema Validation Job");
    }
}
