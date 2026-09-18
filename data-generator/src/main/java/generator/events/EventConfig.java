package generator.events;

import generator.common.EnvLoader;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Instant;
import java.util.Properties;

public class EventConfig {
    // Cluster profile: "plain", "gssapi", "none"
    public String cluster = "plain";
    public String bootstrapServers = null;
    public String securityProtocol = null;
    public String saslMechanism = null;
    public String username = EnvLoader.get("KAFKA_USER", "admin");
    public String password = EnvLoader.get("KAFKA_PASSWORD", "admin-secret");
    public String jaasConfig = null;
    public String keytab = EnvLoader.get("KAFKA_KEYTAB", "/var/lib/secret/client.keytab");
    public String principal = EnvLoader.get("KAFKA_PRINCIPAL", "client@" + EnvLoader.get("KRB5_REALM", "EXAMPLE.COM"));
    public String kerberosServiceName = EnvLoader.get("KAFKA_KERBEROS_SERVICE_NAME", "kafka");
    public String krb5Conf = EnvLoader.get("KRB5_CONF", null);

    public String topic = "events";
    public long numEvents = 1_000_000L;
    public double dirtyRate = 0.05;
    public double lateEventRate = 0.05;
    public int numEntities = 100_000;
    public double dataSkew = 0.5;
    public int workers = 4;
    public boolean continuous = false;
    public Instant startTime = null;
    public Instant appStartTime = Instant.now();

    public EventConfig() {
        resolveBootstrapServers();
    }

    public void resolveBootstrapServers() {
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            String serverIp = EnvLoader.get("SERVER_IP", "127.0.0.1");
            if ("gssapi".equalsIgnoreCase(cluster)) {
                bootstrapServers = serverIp + ":" + EnvLoader.get("KAFKA_GSSAPI_PORT", "9094");
            } else if ("plain".equalsIgnoreCase(cluster)) {
                bootstrapServers = serverIp + ":" + EnvLoader.get("KAFKA_PLAIN_PORT", EnvLoader.get("KAFKA_PORT", "9092"));
            } else {
                bootstrapServers = serverIp + ":" + EnvLoader.get("KAFKA_PORT", "9092");
            }
        }
    }

    public Instant getSimulatedNow() {
        if (startTime == null) return Instant.now();
        long elapsedMillis = Instant.now().toEpochMilli() - appStartTime.toEpochMilli();
        return startTime.plusMillis(elapsedMillis);
    }

    public String getEffectiveSecurityProtocol() {
        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            return securityProtocol;
        }
        if ("none".equalsIgnoreCase(cluster)) {
            return "PLAINTEXT";
        }
        return "SASL_PLAINTEXT";
    }

    public String getEffectiveSaslMechanism() {
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            return saslMechanism;
        }
        if ("gssapi".equalsIgnoreCase(cluster)) {
            return "GSSAPI";
        }
        if ("plain".equalsIgnoreCase(cluster)) {
            return "PLAIN";
        }
        return "NONE";
    }

    public Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024L);

        String effSecProto = getEffectiveSecurityProtocol();
        String effSaslMech = getEffectiveSaslMechanism();

        if (!"PLAINTEXT".equalsIgnoreCase(effSecProto)) {
            props.put("security.protocol", effSecProto);
        }

        if ("GSSAPI".equalsIgnoreCase(effSaslMech)) {
            props.put("sasl.mechanism", "GSSAPI");
            props.put("sasl.kerberos.service.name", kerberosServiceName);
            if (krb5Conf != null && !krb5Conf.trim().isEmpty()) {
                System.setProperty("java.security.krb5.conf", krb5Conf);
            }
            String defaultJaas = String.format(
                    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab=\"%s\" principal=\"%s\";",
                    keytab, principal);
            props.put("sasl.jaas.config", jaasConfig != null ? jaasConfig : defaultJaas);

        } else if ("PLAIN".equalsIgnoreCase(effSaslMech)) {
            props.put("sasl.mechanism", "PLAIN");
            String defaultJaas = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    username, password);
            props.put("sasl.jaas.config", jaasConfig != null ? jaasConfig : defaultJaas);
        }

        return props;
    }

    public static EventConfig parse(String[] args) {
        EventConfig config = new EventConfig();
        boolean customBootstrap = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--cluster":
                    config.cluster = args[++i];
                    break;
                case "--bootstrap-servers":
                    config.bootstrapServers = args[++i];
                    customBootstrap = true;
                    break;
                case "--security-protocol":
                    config.securityProtocol = args[++i];
                    break;
                case "--sasl-mechanism":
                    config.saslMechanism = args[++i];
                    break;
                case "--username":
                case "--user":
                    config.username = args[++i];
                    break;
                case "--password":
                case "--pass":
                    config.password = args[++i];
                    break;
                case "--jaas-config":
                    config.jaasConfig = args[++i];
                    break;
                case "--keytab":
                    config.keytab = args[++i];
                    break;
                case "--principal":
                    config.principal = args[++i];
                    break;
                case "--krb5-conf":
                    config.krb5Conf = args[++i];
                    break;
                case "--topic":
                    config.topic = args[++i];
                    break;
                case "--num-events":
                    config.numEvents = Long.parseLong(args[++i]);
                    break;
                case "--dirty-rate":
                    config.dirtyRate = Double.parseDouble(args[++i]);
                    break;
                case "--late-event-rate":
                    config.lateEventRate = Double.parseDouble(args[++i]);
                    break;
                case "--num-entities":
                    config.numEntities = Integer.parseInt(args[++i]);
                    break;
                case "--data-skew":
                    config.dataSkew = Double.parseDouble(args[++i]);
                    break;
                case "--workers":
                    config.workers = Integer.parseInt(args[++i]);
                    break;
                case "--continuous":
                    config.continuous = true;
                    break;
                case "--start-time":
                    config.startTime = Instant.parse(args[++i]);
                    break;
            }
        }

        if (!customBootstrap) {
            config.bootstrapServers = null;
            config.resolveBootstrapServers();
        }

        return config;
    }

    public void validate() {
        if (!"plain".equalsIgnoreCase(cluster) && !"gssapi".equalsIgnoreCase(cluster) && !"none".equalsIgnoreCase(cluster)) {
            throw new IllegalArgumentException("cluster must be 'plain', 'gssapi', or 'none'");
        }
        if (numEvents <= 0)
            throw new IllegalArgumentException("num-events must be > 0");
        if (dirtyRate < 0 || dirtyRate > 1)
            throw new IllegalArgumentException("dirty-rate must be 0-1");
        if (lateEventRate < 0 || lateEventRate > 1)
            throw new IllegalArgumentException("late-event-rate must be 0-1");
    }
}
