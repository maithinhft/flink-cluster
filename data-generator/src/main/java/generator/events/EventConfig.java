package generator.events;

import generator.common.EnvLoader;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;

public class EventConfig {
    // Cluster mode: "dual" (default multi-cluster routing), "plain", "gssapi", "none"
    public String cluster = "dual";

    // Plain cluster configuration
    public String plainBootstrapServers = null;
    public String username = EnvLoader.get("KAFKA_USER", "admin");
    public String password = EnvLoader.get("KAFKA_PASSWORD", "admin-secret");

    // GSSAPI cluster configuration
    public String gssapiBootstrapServers = null;
    public String keytab = null;
    public String principal = EnvLoader.get("KAFKA_PRINCIPAL", "client@" + EnvLoader.get("KRB5_REALM", "EXAMPLE.COM"));
    public String kerberosServiceName = EnvLoader.get("KAFKA_KERBEROS_SERVICE_NAME", "kafka");
    public String krb5Conf = null;
    public String jaasConfig = null;

    // Generic overrides
    public String bootstrapServers = null;
    public String securityProtocol = null;
    public String saslMechanism = null;

    // Resolved paths for host execution
    private String resolvedKeytab = null;
    private String resolvedKrb5Conf = null;

    // Event generation parameters
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
        String serverIp = EnvLoader.get("SERVER_IP", "127.0.0.1");

        if (plainBootstrapServers == null || plainBootstrapServers.isEmpty()) {
            plainBootstrapServers = EnvLoader.get("KAFKA_PLAIN_BOOTSTRAP_SERVERS", null);
            if (plainBootstrapServers == null || plainBootstrapServers.isEmpty()) {
                String plainPort = EnvLoader.get("KAFKA_PLAIN_PORT", EnvLoader.get("KAFKA_PORT", "9092"));
                plainBootstrapServers = serverIp + ":" + plainPort;
            }
        }

        if (gssapiBootstrapServers == null || gssapiBootstrapServers.isEmpty()) {
            gssapiBootstrapServers = EnvLoader.get("KAFKA_GSSAPI_BOOTSTRAP_SERVERS", null);
            if (gssapiBootstrapServers == null || gssapiBootstrapServers.isEmpty()) {
                String gssapiPort = EnvLoader.get("KAFKA_GSSAPI_PORT", "9094");
                boolean gssapiResolvable = false;
                try {
                    InetAddress.getByName("kafka-gssapi");
                    gssapiResolvable = true;
                } catch (Exception ignored) {
                }

                if (gssapiResolvable) {
                    gssapiBootstrapServers = "kafka-gssapi:" + gssapiPort;
                } else {
                    gssapiBootstrapServers = serverIp + ":" + gssapiPort;
                }
            }
        }

        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            if ("gssapi".equalsIgnoreCase(cluster)) {
                bootstrapServers = gssapiBootstrapServers;
            } else {
                bootstrapServers = plainBootstrapServers;
            }
        }
    }

    public Instant getSimulatedNow() {
        if (startTime == null) return Instant.now();
        long elapsedMillis = Instant.now().toEpochMilli() - appStartTime.toEpochMilli();
        return startTime.plusMillis(elapsedMillis);
    }

    public String getResolvedKeytab() {
        if (resolvedKeytab == null) {
            resolveKerberosFiles();
        }
        return resolvedKeytab;
    }

    public String getResolvedKrb5Conf() {
        if (resolvedKrb5Conf == null) {
            resolveKerberosFiles();
        }
        return resolvedKrb5Conf;
    }

    public synchronized void resolveKerberosFiles() {
        if (resolvedKeytab != null && resolvedKrb5Conf != null) {
            return;
        }

        String rootDir = EnvLoader.getRootDirectory();

        // 1. Resolve keytab
        if (keytab != null && !keytab.trim().isEmpty()) {
            resolvedKeytab = keytab;
        } else {
            String envKeytab = EnvLoader.get("KAFKA_KEYTAB", null);
            if (envKeytab != null && !envKeytab.trim().isEmpty()) {
                resolvedKeytab = envKeytab;
            } else {
                Path[] candidateKeytabs = new Path[] {
                        Paths.get(rootDir, "security", "client.keytab"),
                        Paths.get("security", "client.keytab"),
                        Paths.get("..", "security", "client.keytab"),
                        Paths.get("/var/lib/secret/client.keytab")
                };
                for (Path p : candidateKeytabs) {
                    if (Files.exists(p)) {
                        resolvedKeytab = p.toAbsolutePath().toString();
                        break;
                    }
                }
                if (resolvedKeytab == null) {
                    resolvedKeytab = Paths.get(rootDir, "security", "client.keytab").toAbsolutePath().toString();
                }
            }
        }

        // 2. Resolve krb5.conf
        if (krb5Conf != null && !krb5Conf.trim().isEmpty()) {
            resolvedKrb5Conf = krb5Conf;
        } else {
            String envKrb5 = EnvLoader.get("KRB5_CONF", null);
            if (envKrb5 != null && !envKrb5.trim().isEmpty()) {
                resolvedKrb5Conf = envKrb5;
            } else {
                Path[] candidateConfs = new Path[] {
                        Paths.get(rootDir, "security", "krb5.conf"),
                        Paths.get("security", "krb5.conf"),
                        Paths.get("..", "security", "krb5.conf"),
                        Paths.get("/var/lib/secret/krb5.conf"),
                        Paths.get("/etc/krb5.conf")
                };
                for (Path p : candidateConfs) {
                    if (Files.exists(p)) {
                        resolvedKrb5Conf = p.toAbsolutePath().toString();
                        break;
                    }
                }
                if (resolvedKrb5Conf == null) {
                    resolvedKrb5Conf = Paths.get(rootDir, "security", "krb5.conf").toAbsolutePath().toString();
                }
            }
        }

        // 3. Normalize KDC hostname and settings for host execution
        checkAndNormalizeKdcConf();
    }

    private void checkAndNormalizeKdcConf() {
        if (resolvedKrb5Conf == null) return;
        Path krb5Path = Paths.get(resolvedKrb5Conf);
        if (!Files.exists(krb5Path)) return;

        boolean kdcResolvable = false;
        try {
            InetAddress.getByName("kdc");
            kdcResolvable = true;
        } catch (Exception ignored) {
        }

        try {
            String content = Files.readString(krb5Path);
            boolean needsPatch = false;
            String patched = content;

            // Enforce TCP preference and timeout if missing
            if (!patched.contains("udp_preference_limit")) {
                patched = patched.replace("[libdefaults]", "[libdefaults]\n    udp_preference_limit = 1\n    kdc_timeout = 5000");
                needsPatch = true;
            }

            if (!kdcResolvable && (patched.contains("kdc:88") || patched.contains("kdc:749"))) {
                String serverIp = EnvLoader.get("SERVER_IP", "127.0.0.1");
                patched = patched
                        .replace("kdc:88", serverIp + ":88")
                        .replace("kdc:749", serverIp + ":749");
                needsPatch = true;
            }

            if (needsPatch) {
                try {
                    Files.writeString(krb5Path, patched);
                } catch (Exception writeEx) {
                    Path hostKrb5 = krb5Path.getParent() != null
                            ? krb5Path.getParent().resolve("krb5_host.conf")
                            : Paths.get("krb5_host.conf");
                    Files.writeString(hostKrb5, patched);
                    resolvedKrb5Conf = hostKrb5.toAbsolutePath().toString();
                }
            }
        } catch (IOException e) {
            System.err.println("Notice: Could not inspect/normalize krb5.conf: " + e.getMessage());
        }
    }

    public void initSecurity() {
        if ("dual".equalsIgnoreCase(cluster) || "multi".equalsIgnoreCase(cluster) || "gssapi".equalsIgnoreCase(cluster)) {
            System.setProperty("sun.security.krb5.canonHost", "false");
            resolveKerberosFiles();
            if (resolvedKrb5Conf != null && Files.exists(Paths.get(resolvedKrb5Conf))) {
                System.setProperty("java.security.krb5.conf", resolvedKrb5Conf);
            }
        }
    }

    public Properties getPlainProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, plainBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024L);

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        String defaultJaas = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                username, password);
        props.put("sasl.jaas.config", defaultJaas);

        return props;
    }

    public Properties getGssapiProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, gssapiBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024L);

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", kerberosServiceName);

        String keytabPath = getResolvedKeytab();
        String defaultJaas = String.format(
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab=\"%s\" principal=\"%s\";",
                keytabPath, principal);
        props.put("sasl.jaas.config", jaasConfig != null ? jaasConfig : defaultJaas);

        return props;
    }

    public Properties getProducerProperties() {
        if ("gssapi".equalsIgnoreCase(cluster)) {
            return getGssapiProducerProperties();
        } else if ("none".equalsIgnoreCase(cluster)) {
            Properties props = getPlainProducerProperties();
            props.put("security.protocol", "PLAINTEXT");
            props.remove("sasl.mechanism");
            props.remove("sasl.jaas.config");
            return props;
        } else {
            return getPlainProducerProperties();
        }
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
                case "--plain-bootstrap":
                case "--plain-bootstrap-servers":
                    config.plainBootstrapServers = args[++i];
                    break;
                case "--gssapi-bootstrap":
                case "--gssapi-bootstrap-servers":
                    config.gssapiBootstrapServers = args[++i];
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

        if (customBootstrap) {
            if ("gssapi".equalsIgnoreCase(config.cluster)) {
                config.gssapiBootstrapServers = config.bootstrapServers;
            } else if ("plain".equalsIgnoreCase(config.cluster) || "none".equalsIgnoreCase(config.cluster)) {
                config.plainBootstrapServers = config.bootstrapServers;
            }
        }

        return config;
    }

    public void validate() {
        if (!"dual".equalsIgnoreCase(cluster) && !"multi".equalsIgnoreCase(cluster) &&
                !"plain".equalsIgnoreCase(cluster) && !"gssapi".equalsIgnoreCase(cluster) &&
                !"none".equalsIgnoreCase(cluster)) {
            throw new IllegalArgumentException("cluster must be 'dual', 'multi', 'plain', 'gssapi', or 'none'");
        }
        if (numEvents <= 0)
            throw new IllegalArgumentException("num-events must be > 0");
        if (dirtyRate < 0 || dirtyRate > 1)
            throw new IllegalArgumentException("dirty-rate must be 0-1");
        if (lateEventRate < 0 || lateEventRate > 1)
            throw new IllegalArgumentException("late-event-rate must be 0-1");

        // Validate Kerberos files if GSSAPI is needed
        if ("dual".equalsIgnoreCase(cluster) || "multi".equalsIgnoreCase(cluster) || "gssapi".equalsIgnoreCase(cluster)) {
            String kt = getResolvedKeytab();
            if (kt == null || !Files.exists(Paths.get(kt))) {
                System.err.printf("[WARN] Kerberos keytab not found at: %s%n" +
                        "       If connecting to kafka-gssapi fails, run: ./up.script.sh or " +
                        "'docker compose cp kdc:/var/lib/secret/client.keytab ./security/client.keytab'%n", kt);
            }

            boolean gssapiResolvable = false;
            try {
                InetAddress.getByName("kafka-gssapi");
                gssapiResolvable = true;
            } catch (Exception ignored) {
            }

            String serverIp = EnvLoader.get("SERVER_IP", "127.0.0.1");
            if (!gssapiResolvable && !"127.0.0.1".equals(serverIp) && !"localhost".equalsIgnoreCase(serverIp)) {
                System.out.println("----------------------------------------------------------------------");
                System.out.println("[NOTICE for Kerberos GSSAPI Authentication]");
                System.out.println("  If connection to " + gssapiBootstrapServers + " is terminated during authentication,");
                System.out.println("  please map hostname 'kafka-gssapi' to your server IP in /etc/hosts:");
                System.out.println("    echo \"" + serverIp + " kafka-gssapi\" | sudo tee -a /etc/hosts");
                System.out.println("----------------------------------------------------------------------");
            }
        }
    }
}
