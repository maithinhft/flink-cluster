package flink.dynamic.metadata;

import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class PostgresKafkaMetadataServiceTest {

    @Test
    public void testEmptyStreamIdsReturnsEmptyMap() {
        PostgresKafkaMetadataService service = new PostgresKafkaMetadataService(
                "jdbc:postgresql://invalid-host:5432/db", "user", "pass");

        Map<String, KafkaStream> streams = service.describeStreams(Collections.emptyList());
        assertNotNull(streams);
        assertTrue(streams.isEmpty());
    }

    @Test
    public void testNullStreamIdsReturnsEmptyMap() {
        PostgresKafkaMetadataService service = new PostgresKafkaMetadataService(
                "jdbc:postgresql://invalid-host:5432/db", "user", "pass");

        Map<String, KafkaStream> streams = service.describeStreams(null);
        assertNotNull(streams);
        assertTrue(streams.isEmpty());
    }

    @Test
    public void testDatabaseConnectionFailureReturnsEmptyGracefully() {
        PostgresKafkaMetadataService service = new PostgresKafkaMetadataService(
                "jdbc:postgresql://nonexistent-host:5432/test", "user", "pass", "kafka_stream", 1000L);

        Set<KafkaStream> allStreams = service.getAllStreams();
        assertNotNull(allStreams);
        assertTrue(allStreams.isEmpty());

        Map<String, KafkaStream> described = service.describeStreams(Collections.singletonList("stream-events"));
        assertNotNull(described);
        assertTrue(described.isEmpty());
    }

    @Test
    public void testIsClusterActiveDefault() {
        PostgresKafkaMetadataService service = new PostgresKafkaMetadataService(
                "jdbc:postgresql://nonexistent-host:5432/test", "user", "pass");

        assertFalse(service.isClusterActive(null));
        assertTrue(service.isClusterActive("kafka-plain"));
    }
}

