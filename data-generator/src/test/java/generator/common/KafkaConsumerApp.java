package generator.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

public class KafkaConsumerApp {

    public static void main(String[] args) {
        String topic = null;
        int maxMessages = 10; // Default read 10 messages

        String cluster = null;
        String customBootstrap = null;

        for (int i = 0; i < args.length; i++) {
            if ("--topic".equals(args[i]) && i + 1 < args.length) {
                topic = args[i + 1];
            } else if ("--max".equals(args[i]) && i + 1 < args.length) {
                try {
                    maxMessages = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException ignored) {
                }
            } else if ("--cluster".equals(args[i]) && i + 1 < args.length) {
                cluster = args[i + 1];
            } else if ("--bootstrap-servers".equals(args[i]) && i + 1 < args.length) {
                customBootstrap = args[i + 1];
            }
        }

        if (topic == null) {
            System.err.println(
                    "Usage: java -cp target/... generator.common.KafkaConsumerApp --topic <topic_name> [--max <number_of_messages>] [--cluster <plain|gssapi|none>] [--bootstrap-servers <host:port>]");
            System.exit(1);
        }

        if (cluster == null) {
            cluster = "schema_registry".equalsIgnoreCase(topic) ? "gssapi" : "plain";
        }

        String serverIp = EnvLoader.get("SERVER_IP", "127.0.0.1");
        String bootstrapServers = customBootstrap;
        if (bootstrapServers == null) {
            if ("gssapi".equalsIgnoreCase(cluster)) {
                bootstrapServers = serverIp + ":" + EnvLoader.get("KAFKA_GSSAPI_PORT", "9094");
            } else if ("plain".equalsIgnoreCase(cluster)) {
                bootstrapServers = serverIp + ":" + EnvLoader.get("KAFKA_PLAIN_PORT", EnvLoader.get("KAFKA_PORT", "9092"));
            } else {
                bootstrapServers = serverIp + ":" + EnvLoader.get("KAFKA_PORT", "9092");
            }
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        if ("gssapi".equalsIgnoreCase(cluster)) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "GSSAPI");
            props.put("sasl.kerberos.service.name", "kafka");
            String krb5 = EnvLoader.get("KRB5_CONF", null);
            if (krb5 != null && !krb5.isEmpty()) {
                System.setProperty("java.security.krb5.conf", krb5);
            }
            String keytab = EnvLoader.get("KAFKA_KEYTAB", "/var/lib/secret/client.keytab");
            String principal = EnvLoader.get("KAFKA_PRINCIPAL", "client@" + EnvLoader.get("KRB5_REALM", "EXAMPLE.COM"));
            props.put("sasl.jaas.config", String.format(
                    "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab=\"%s\" principal=\"%s\";",
                    keytab, principal));
        } else if ("plain".equalsIgnoreCase(cluster)) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "PLAIN");
            String user = EnvLoader.get("KAFKA_USER", "admin");
            String pass = EnvLoader.get("KAFKA_PASSWORD", "admin-secret");
            props.put("sasl.jaas.config", String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    user, pass));
        }

        System.out.println("Connecting to Kafka Cluster: " + cluster.toUpperCase() + " (" + bootstrapServers + ")");
        System.out.println("Consuming topic: " + topic);
        System.out.println("Max messages to read: " + maxMessages);

        int count = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Thay vì dùng subscribe (cần Consumer Group Coordinator), ta dùng assign để
            // đọc trực tiếp
            java.util.List<org.apache.kafka.common.PartitionInfo> partitions = consumer.partitionsFor(topic);
            java.util.List<org.apache.kafka.common.TopicPartition> topicPartitions = new java.util.ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo p : partitions) {
                topicPartitions.add(new org.apache.kafka.common.TopicPartition(topic, p.partition()));
            }
            consumer.assign(topicPartitions);
            // Đọc từ CUỐI (latest - N) thay vì từ đầu để tránh bị treo nếu topic có quá
            // nhiều dữ liệu cũ
            java.util.Map<org.apache.kafka.common.TopicPartition, Long> endOffsets = consumer
                    .endOffsets(topicPartitions);
            for (org.apache.kafka.common.TopicPartition tp : topicPartitions) {
                long endOffset = endOffsets.get(tp);
                long startOffset = Math.max(0, endOffset - maxMessages);
                consumer.seek(tp, startOffset);
            }

            System.out.println("Assigned to " + topicPartitions.size() + " partitions. Seeking to end offsets...");

            if (!partitions.isEmpty()) {
                org.apache.kafka.common.Node leader = partitions.get(0).leader();
                System.out.println(
                        "DEBUG: Leader for partition 0 is advertised as: " + leader.host() + ":" + leader.port());
            }

            int emptyPolls = 0;
            while (count < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));

                if (records.isEmpty()) {
                    emptyPolls++;
                    if (emptyPolls % 5 == 0) {
                        System.out.println("... still waiting for messages (polled " + emptyPolls + " times)");
                    }
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("----------------------------------------");
                    System.out.println("Key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());

                    count++;
                    if (count >= maxMessages) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading from Kafka: " + e.getMessage());
        }

        System.out.println("----------------------------------------");
        System.out.println("Finished reading " + count + " messages.");
    }
}
