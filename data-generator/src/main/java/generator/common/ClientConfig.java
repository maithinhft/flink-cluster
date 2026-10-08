package generator.common;

import generator.events.EventConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Shared Kafka consumer / PostgreSQL settings for the tool apps (result sink, latency probe, verifier, demo).
 * Security settings (SASL PLAIN or Kerberos GSSAPI) are taken from {@link EventConfig} so every tool talks to the
 * two clusters exactly like the event generator does.
 */
public final class ClientConfig {

    private ClientConfig() {
    }

    public static EventConfig eventConfig(String[] args) {
        EventConfig c = EventConfig.parse(args);
        c.initSecurity();
        return c;
    }

    public static Properties consumer(EventConfig cfg, boolean gssapi, String groupId) {
        Properties producer = gssapi ? cfg.getGssapiProducerProperties() : cfg.getPlainProducerProperties();
        Properties p = new Properties();
        for (String k : producer.stringPropertyNames()) {
            if (k.startsWith("security.") || k.startsWith("sasl.") || k.equals("bootstrap.servers")) {
                p.put(k, producer.getProperty(k));
            }
        }
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
        if (groupId != null) {
            p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    public static String dbUrl() {
        return "jdbc:postgresql://" + EnvLoader.get("SERVER_IP", "127.0.0.1") + ":"
                + EnvLoader.get("POSTGRES_PORT", "5433") + "/" + EnvLoader.get("POSTGRES_DB", "realtime_core");
    }

    public static Connection db(String url) throws SQLException {
        return DriverManager.getConnection(url == null ? dbUrl() : url,
                EnvLoader.get("POSTGRES_USER", "postgres"), EnvLoader.get("POSTGRES_PASSWORD", "postgres"));
    }

    public static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return def;
    }

    public static boolean flag(String[] args, String name) {
        for (String a : args) {
            if (name.equals(a)) {
                return true;
            }
        }
        return false;
    }
}
