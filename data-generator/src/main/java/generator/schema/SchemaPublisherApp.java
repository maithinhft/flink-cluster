package generator.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import generator.common.EnvLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

public class SchemaPublisherApp {

    private static final String TOPIC_NAME = "schema_registry";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        String pathArg = null;
        for (int i = 0; i < args.length; i++) {
            if ("--path".equals(args[i]) && i + 1 < args.length) {
                pathArg = args[i + 1];
                break;
            }
        }

        if (pathArg == null) {
            System.err.println(
                    "Usage: java -cp target/... generator.schema.SchemaPublisherApp --path <file_or_directory>");
            System.exit(1);
        }

        Path path = Paths.get(pathArg);
        if (!Files.exists(path)) {
            System.err.println("Error: Path does not exist: " + pathArg);
            System.exit(1);
        }

        // Setup Kafka Producer
        Properties props = new Properties();
        String bootstrapServers = EnvLoader.get("SERVER_IP", "127.0.0.1") + ":" + EnvLoader.get("KAFKA_PORT", "9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.println("Connecting to Kafka: " + bootstrapServers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            if (Files.isDirectory(path)) {
                try (Stream<Path> paths = Files.walk(path)) {
                    paths.filter(Files::isRegularFile)
                            .filter(p -> p.toString().endsWith(".json"))
                            .forEach(p -> processAndSend(p.toFile(), producer));
                }
            } else {
                processAndSend(path.toFile(), producer);
            }
        } catch (IOException e) {
            System.err.println("Failed to walk the directory: " + e.getMessage());
        }

        System.out.println("Done processing schemas.");
    }

    private static void processAndSend(File jsonFile, KafkaProducer<String, String> producer) {
        try {
            JsonNode rootNode = mapper.readTree(jsonFile);

            // Check if it's a unified schema or action schema
            String sourceName = rootNode.has("source_name") ? rootNode.get("source_name").asText() : "unknown";
            String action = rootNode.has("action") ? rootNode.get("action").asText() : null;

            String kafkaKey;
            if ("unified_dictionary".equals(rootNode.has("name") ? rootNode.get("name").asText() : "")) {
                kafkaKey = "unified_schema";
            } else if (action != null) {
                kafkaKey = sourceName + "_" + action;
            } else {
                kafkaKey = sourceName + "_schema";
            }

            JsonNode fieldsNode = rootNode.get("fields");
            if (fieldsNode != null && fieldsNode.isObject()) {
                ObjectNode fieldsObj = (ObjectNode) fieldsNode;
                fieldsObj.fieldNames().forEachRemaining(fieldName -> {
                    JsonNode fieldDefNode = fieldsObj.get(fieldName);
                    if (fieldDefNode.isObject()) {
                        ((ObjectNode) fieldDefNode).remove("description");
                    }
                });
            }

            String minifiedJson = mapper.writeValueAsString(rootNode);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, kafkaKey, minifiedJson);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending schema for key " + kafkaKey + ": " + exception.getMessage());
                } else {
                    System.out.println("Sent schema to partition " + metadata.partition() + " with key: " + kafkaKey);
                }
            });

        } catch (IOException e) {
            System.err.println("Failed to process file " + jsonFile.getAbsolutePath() + ": " + e.getMessage());
        }
    }
}
