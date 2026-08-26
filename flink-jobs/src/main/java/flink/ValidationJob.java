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

        // 1. Sử dụng ParameterTool để đọc tham số truyền vào từ lúc submit Job (qua CLI
        // hoặc Web UI)
        ParameterTool parameters = ParameterTool.fromArgs(args);

        // Đăng ký ParameterTool làm Global Job Parameters để mọi Operator đều có thể
        // truy cập nếu cần
        env.getConfig().setGlobalJobParameters(parameters);

        // Đọc cấu hình từ Tham số dòng lệnh (--bootstrap.servers)
        String bootstrapServers = parameters.get("bootstrap.servers", "kafka:29092");

        String eventsTopicPattern = parameters.get("events.topic.pattern", "events.*");
        String schemaTopic = parameters.get("schema.topic", "schema_registry");

        LOG.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        LOG.info("Events Topic Pattern: {}", eventsTopicPattern);
        LOG.info("Schema Topic: {}", schemaTopic);

        // 1. Tạo Kafka Source để đọc Schema
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

        // Biến luồng Schema thành Broadcast Stream
        BroadcastStream<String> broadcastSchemaStream = schemaStream
                .broadcast(
                        SchemaValidationBroadcastProcessFunction.SCHEMA_STATE_DESCRIPTOR,
                        SchemaValidationBroadcastProcessFunction.LATEST_VERSION_DESCRIPTOR,
                        SchemaValidationBroadcastProcessFunction.DEPRECATED_SCHEMAS_DESCRIPTOR
                );

        // 2. Tạo Kafka Source để đọc Event Data từ NHIỀU TOPIC dựa trên Regex Pattern
        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(java.util.regex.Pattern.compile(eventsTopicPattern))
                .setGroupId("flink-event-validation-group")
                // Quan trọng: Bật tính năng tự động tìm kiếm topic/partition mới mỗi 60 giây (60000ms)
                .setProperty("partition.discovery.interval.ms", "60000")
                // Trong thực tế có thể đọc từ earliest hoặc latest. Ở đây để dễ demo ta đọc từ earliest
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> eventStream = env.fromSource(
                eventSource,
                WatermarkStrategy.noWatermarks(),
                "Events Source");

        // 3. Kết nối luồng Event với luồng Schema (Broadcast) và xử lý
        SingleOutputStreamOperator<String> processedStream = eventStream
                .connect(broadcastSchemaStream)
                .process(new SchemaValidationBroadcastProcessFunction())
                .name("Schema Validation Operator");

        // 4. In các luồng ra màn hình

        // Luồng dữ liệu bẩn (Side Output)
        DataStream<String> dirtyEventsStream = processedStream
                .getSideOutput(SchemaValidationBroadcastProcessFunction.DIRTY_DATA_TAG);
        dirtyEventsStream.map(data -> {
            LOG.info("DIRTY DATA -> {}", data);
            return data;
        });

        // Luồng dữ liệu sạch
        processedStream.map(data -> {
            LOG.info("VALID DATA -> {}", data);
            return data;
        });

        env.execute("Flink Realtime Schema Validation Job");
    }
}
