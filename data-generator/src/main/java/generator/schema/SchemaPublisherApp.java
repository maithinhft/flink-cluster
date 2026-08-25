package generator.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import generator.common.EnvLoader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Stream;

public class SchemaPublisherApp {

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

        // Setup PostgreSQL Connection
        String serverIp = EnvLoader.get("SERVER_IP", "127.0.0.1");
        String dbPort = EnvLoader.get("POSTGRES_PORT", "5433");
        String dbName = EnvLoader.get("POSTGRES_DB", "realtime_core");
        String dbUrl = "jdbc:postgresql://" + serverIp + ":" + dbPort + "/" + dbName;
        String dbUser = EnvLoader.get("POSTGRES_USER", "postgres");
        String dbPassword = EnvLoader.get("POSTGRES_PASSWORD", "postgres");

        System.out.println("Connecting to PostgreSQL: " + dbUrl);

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
            if (Files.isDirectory(path)) {
                try (Stream<Path> paths = Files.walk(path)) {
                    paths.filter(Files::isRegularFile)
                            .filter(p -> p.toString().endsWith(".json"))
                            .forEach(p -> processAndSend(p.toFile(), conn));
                }
            } else {
                processAndSend(path.toFile(), conn);
            }
        } catch (SQLException e) {
            System.err.println("Failed to connect to PostgreSQL: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Failed to walk the directory: " + e.getMessage());
        }

        System.out.println("Done processing schemas.");
    }

    private static void processAndSend(File jsonFile, Connection conn) {
        try {
            JsonNode rootNode = mapper.readTree(jsonFile);

            // Check if it's a unified schema or action schema
            String sourceName = rootNode.has("source_name") ? rootNode.get("source_name").asText() : "unknown";
            String action = rootNode.has("action") ? rootNode.get("action").asText() : null;

            String schemaId;
            if ("unified_dictionary".equals(rootNode.has("name") ? rootNode.get("name").asText() : "")) {
                schemaId = "unified_schema";
            } else if (action != null) {
                schemaId = sourceName + "_" + action;
            } else {
                schemaId = sourceName + "_schema";
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

            // UPSERT statement (INSERT OR UPDATE)
            String sql = "INSERT INTO schema_definitions (schema_id, schema_payload) " +
                    "VALUES (?, ?::jsonb) " +
                    "ON CONFLICT (schema_id) DO UPDATE " +
                    "SET schema_payload = EXCLUDED.schema_payload, updated_at = CURRENT_TIMESTAMP";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, schemaId);
                pstmt.setString(2, minifiedJson);
                pstmt.executeUpdate();
                System.out.println("Saved schema to DB with ID: " + schemaId);
            } catch (SQLException e) {
                System.err.println("Database error for schema " + schemaId + ": " + e.getMessage());
            }

        } catch (IOException e) {
            System.err.println("Failed to process file " + jsonFile.getAbsolutePath() + ": " + e.getMessage());
        }
    }
}
