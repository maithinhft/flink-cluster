package flink.dynamic.metadata;

import org.apache.flink.connector.kafka.dynamic.metadata.ClusterMetadata;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaMetadataService;
import org.apache.flink.connector.kafka.dynamic.metadata.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Cài đặt KafkaMetadataService đọc thông tin Kafka Cluster và Topics từ PostgreSQL.
 * Hỗ trợ cho DynamicKafkaSource tự động cập nhật topic và cụm Kafka mà không cần khởi động lại job.
 */
public class PostgresKafkaMetadataService implements KafkaMetadataService {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresKafkaMetadataService.class);

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            LOG.warn("PostgreSQL JDBC driver not found on classpath", e);
        }
    }

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String tablePrefix;
    private final long cacheTtlMs;

    private transient volatile Map<String, KafkaStream> cachedStreams;
    private transient volatile long lastFetchTime = 0L;

    public PostgresKafkaMetadataService(String dbUrl, String dbUser, String dbPassword) {
        this(dbUrl, dbUser, dbPassword, "kafka_stream", 5000L);
    }

    public PostgresKafkaMetadataService(String dbUrl, String dbUser, String dbPassword, String tablePrefix) {
        this(dbUrl, dbUser, dbPassword, tablePrefix, 5000L);
    }

    public PostgresKafkaMetadataService(String dbUrl, String dbUser, String dbPassword, String tablePrefix, long cacheTtlMs) {
        this.dbUrl = Objects.requireNonNull(dbUrl, "dbUrl must not be null");
        this.dbUser = Objects.requireNonNull(dbUser, "dbUser must not be null");
        this.dbPassword = dbPassword != null ? dbPassword : "";
        this.tablePrefix = (tablePrefix != null && !tablePrefix.trim().isEmpty()) ? tablePrefix.trim() : "kafka_stream";
        this.cacheTtlMs = cacheTtlMs > 0 ? cacheTtlMs : 5000L;
    }

    @Override
    public synchronized Set<KafkaStream> getAllStreams() {
        Map<String, KafkaStream> map = getOrRefreshStreams();
        return new HashSet<>(map.values());
    }

    @Override
    public synchronized Map<String, KafkaStream> describeStreams(Collection<String> streamIds) {
        if (streamIds == null || streamIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, KafkaStream> all = getOrRefreshStreams();
        Map<String, KafkaStream> result = new HashMap<>();
        for (String id : streamIds) {
            KafkaStream stream = all.get(id);
            if (stream != null) {
                result.put(id, stream);
            } else {
                LOG.warn("Stream ID '{}' not found in PostgreSQL metadata ({}_cluster_config)", id, tablePrefix);
            }
        }
        return result;
    }

    @Override
    public boolean isClusterActive(String clusterId) {
        if (clusterId == null) {
            return false;
        }
        Map<String, KafkaStream> all = getOrRefreshStreams();
        for (KafkaStream stream : all.values()) {
            if (stream.getClusterMetadataMap().containsKey(clusterId)) {
                return true;
            }
        }
        return true;
    }

    @Override
    public void close() {
        // Stateless per call - không cần giữ connection mở liên tục
    }

    /**
     * Lấy ClusterMetadata dựa theo cluster_name từ bảng cấu hình
     */
    public ClusterMetadata getClusterMetadataByClusterName(String clusterName) {
        if (clusterName == null) return null;
        Map<String, KafkaStream> all = getOrRefreshStreams();
        for (KafkaStream stream : all.values()) {
            ClusterMetadata meta = stream.getClusterMetadataMap().get(clusterName);
            if (meta != null) {
                return meta;
            }
        }
        return null;
    }

    /**
     * Lấy ClusterMetadata dựa theo stream_id từ bảng cấu hình
     */
    public ClusterMetadata getClusterMetadataByStreamId(String streamId) {
        if (streamId == null) return null;
        Map<String, KafkaStream> all = getOrRefreshStreams();
        KafkaStream stream = all.get(streamId);
        if (stream != null && !stream.getClusterMetadataMap().isEmpty()) {
            return stream.getClusterMetadataMap().values().iterator().next();
        }
        return null;
    }

    private Map<String, KafkaStream> getOrRefreshStreams() {
        long now = System.currentTimeMillis();
        if (cachedStreams != null && (now - lastFetchTime < cacheTtlMs)) {
            return cachedStreams;
        }

        try {
            Map<String, KafkaStream> fresh = queryStreamsFromDatabase();
            cachedStreams = fresh;
            lastFetchTime = now;
            return fresh;
        } catch (Exception e) {
            LOG.error("Failed to query Kafka stream metadata from PostgreSQL ({}), using cached data if available", dbUrl, e);
            if (cachedStreams != null) {
                return cachedStreams;
            }
            return Collections.emptyMap();
        }
    }

    private Map<String, KafkaStream> queryStreamsFromDatabase() throws SQLException {
        String query = String.format(
                "SELECT " +
                "    c.stream_id, " +
                "    c.cluster_name, " +
                "    c.bootstrap_servers, " +
                "    c.security_protocol, " +
                "    c.sasl_mechanism, " +
                "    c.sasl_kerberos_service_name, " +
                "    c.sasl_jaas_config, " +
                "    t.topic_name, " +
                "    t.enabled AS topic_enabled " +
                "FROM %s_cluster_config c " +
                "LEFT JOIN %s_topic_config t ON c.id = t.cluster_config_id AND t.enabled = TRUE " +
                "WHERE c.enabled = TRUE " +
                "ORDER BY c.stream_id, c.cluster_name",
                tablePrefix, tablePrefix
        );

        // Map: streamId -> Map<clusterName, ClusterDataBuilder>
        Map<String, Map<String, ClusterDataBuilder>> streamBuilders = new HashMap<>();

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement ps = conn.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String streamId = rs.getString("stream_id");
                String clusterName = rs.getString("cluster_name");
                String bootstrapServers = rs.getString("bootstrap_servers");
                String securityProtocol = rs.getString("security_protocol");
                String saslMechanism = rs.getString("sasl_mechanism");
                String saslKerberosServiceName = rs.getString("sasl_kerberos_service_name");
                String saslJaasConfig = rs.getString("sasl_jaas_config");
                String topicName = rs.getString("topic_name");
                boolean topicEnabled = rs.getBoolean("topic_enabled");

                Map<String, ClusterDataBuilder> clustersForStream = streamBuilders.computeIfAbsent(streamId, k -> new HashMap<>());
                ClusterDataBuilder builder = clustersForStream.computeIfAbsent(clusterName, k -> {
                    Properties props = new Properties();
                    props.setProperty("bootstrap.servers", bootstrapServers);
                    if (securityProtocol != null && !securityProtocol.trim().isEmpty()) {
                        props.setProperty("security.protocol", securityProtocol.trim());
                    }
                    if (saslMechanism != null && !saslMechanism.trim().isEmpty()) {
                        props.setProperty("sasl.mechanism", saslMechanism.trim());
                    }
                    if (saslKerberosServiceName != null && !saslKerberosServiceName.trim().isEmpty()) {
                        props.setProperty("sasl.kerberos.service.name", saslKerberosServiceName.trim());
                    }
                    if (saslJaasConfig != null && !saslJaasConfig.trim().isEmpty()) {
                        props.setProperty("sasl.jaas.config", saslJaasConfig.trim());
                    }
                    return new ClusterDataBuilder(props);
                });

                if (topicName != null && topicEnabled) {
                    builder.topics.add(topicName.trim());
                }
            }
        }

        Map<String, KafkaStream> resultMap = new HashMap<>();
        for (Map.Entry<String, Map<String, ClusterDataBuilder>> entry : streamBuilders.entrySet()) {
            String streamId = entry.getKey();
            Map<String, ClusterMetadata> clusterMetadataMap = new HashMap<>();

            for (Map.Entry<String, ClusterDataBuilder> cEntry : entry.getValue().entrySet()) {
                String clusterName = cEntry.getKey();
                ClusterDataBuilder cData = cEntry.getValue();
                clusterMetadataMap.put(clusterName, new ClusterMetadata(cData.topics, cData.properties));
            }

            resultMap.put(streamId, new KafkaStream(streamId, clusterMetadataMap));
            LOG.info("Discovered KafkaStream '{}' with clusters: {}", streamId, clusterMetadataMap.keySet());
        }

        return resultMap;
    }

    private static class ClusterDataBuilder {
        final Properties properties;
        final Set<String> topics = new HashSet<>();

        ClusterDataBuilder(Properties properties) {
            this.properties = properties;
        }
    }
}
