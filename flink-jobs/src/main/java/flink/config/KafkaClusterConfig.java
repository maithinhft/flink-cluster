package flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Properties;

public class KafkaClusterConfig {

    public static final String CLUSTER_PLAIN = "plain";
    public static final String CLUSTER_GSSAPI = "gssapi";
    public static final String CLUSTER_NONE = "none";

    /**
     * Lấy bootstrap servers cho stream cụ thể (ví dụ: "schema", "rule", "events", "result", "dlq")
     */
    public static String getBootstrapServers(ParameterTool params, String streamPrefix, String defaultCluster) {
        String clusterType = params.get(streamPrefix + ".cluster", defaultCluster);

        // 1. Kiểm tra nếu có ghi đè trực tiếp cho stream: ví dụ --schema.bootstrap.servers
        if (params.has(streamPrefix + ".bootstrap.servers")) {
            return params.get(streamPrefix + ".bootstrap.servers");
        }

        // 2. Lấy theo cluster type
        if (CLUSTER_GSSAPI.equalsIgnoreCase(clusterType)) {
            return params.get("kafka.gssapi.bootstrap.servers",
                    params.get("gssapi.bootstrap.servers", "kafka-gssapi:29094"));
        } else if (CLUSTER_PLAIN.equalsIgnoreCase(clusterType)) {
            return params.get("kafka.plain.bootstrap.servers",
                    params.get("plain.bootstrap.servers",
                    params.get("bootstrap.servers", "kafka-plain:29092")));
        } else {
            return params.get("bootstrap.servers", "kafka-plain:29092");
        }
    }

    /**
     * Tạo Properties bảo mật cho KafkaSource (Consumer)
     */
    public static Properties getConsumerProperties(ParameterTool params, String streamPrefix, String defaultCluster) {
        return buildProperties(params, streamPrefix, defaultCluster, false);
    }

    /**
     * Tạo Properties bảo mật cho KafkaSink (Producer)
     */
    public static Properties getProducerProperties(ParameterTool params, String streamPrefix, String defaultCluster) {
        return buildProperties(params, streamPrefix, defaultCluster, true);
    }

    private static Properties buildProperties(ParameterTool params, String streamPrefix, String defaultCluster, boolean isProducer) {
        Properties props = new Properties();
        String clusterType = params.get(streamPrefix + ".cluster", defaultCluster);

        String bootstrapServers = getBootstrapServers(params, streamPrefix, defaultCluster);
        props.setProperty("bootstrap.servers", bootstrapServers);

        if (CLUSTER_GSSAPI.equalsIgnoreCase(clusterType)) {
            props.setProperty("security.protocol", params.get(streamPrefix + ".security.protocol",
                    params.get("kafka.gssapi.security.protocol", "SASL_PLAINTEXT")));
            props.setProperty("sasl.mechanism", params.get(streamPrefix + ".sasl.mechanism",
                    params.get("kafka.gssapi.sasl.mechanism", "GSSAPI")));
            props.setProperty("sasl.kerberos.service.name", params.get(streamPrefix + ".sasl.kerberos.service.name",
                    params.get("kafka.gssapi.sasl.kerberos.service.name", "kafka")));

            String keytab = params.get(streamPrefix + ".keytab",
                    params.get("kafka.gssapi.keytab", "/var/lib/secret/client.keytab"));
            String principal = params.get(streamPrefix + ".principal",
                    params.get("kafka.gssapi.principal", "client@EXAMPLE.COM"));

            String defaultJaas = String.format(
                    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab=\"%s\" principal=\"%s\";",
                    keytab, principal
            );
            props.setProperty("sasl.jaas.config", params.get(streamPrefix + ".sasl.jaas.config",
                    params.get("kafka.gssapi.sasl.jaas.config", defaultJaas)));

        } else if (CLUSTER_PLAIN.equalsIgnoreCase(clusterType)) {
            props.setProperty("security.protocol", params.get(streamPrefix + ".security.protocol",
                    params.get("kafka.plain.security.protocol", "SASL_PLAINTEXT")));
            props.setProperty("sasl.mechanism", params.get(streamPrefix + ".sasl.mechanism",
                    params.get("kafka.plain.sasl.mechanism", "PLAIN")));

            String user = params.get(streamPrefix + ".username",
                    params.get("kafka.plain.username", "admin"));
            String pass = params.get(streamPrefix + ".password",
                    params.get("kafka.plain.password", "admin-secret"));

            String defaultJaas = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    user, pass
            );
            props.setProperty("sasl.jaas.config", params.get(streamPrefix + ".sasl.jaas.config",
                    params.get("kafka.plain.sasl.jaas.config", defaultJaas)));
        }

        return props;
    }
}

