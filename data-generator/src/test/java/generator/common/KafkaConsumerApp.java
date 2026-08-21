package generator.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {

    public static void main(String[] args) {
        String topic = null;
        int maxMessages = 10; // Default read 10 messages

        for (int i = 0; i < args.length; i++) {
            if ("--topic".equals(args[i]) && i + 1 < args.length) {
                topic = args[i + 1];
            } else if ("--max".equals(args[i]) && i + 1 < args.length) {
                try {
                    maxMessages = Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException ignored) {}
            }
        }

        if (topic == null) {
            System.err.println("Usage: java -cp target/... generator.common.KafkaConsumerApp --topic <topic_name> [--max <number_of_messages>]");
            System.exit(1);
        }

        String bootstrapServers = EnvLoader.get("SERVER_IP", "127.0.0.1") + ":" + EnvLoader.get("KAFKA_PORT", "9092");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Đọc từ đầu

        System.out.println("Connecting to Kafka: " + bootstrapServers);
        System.out.println("Consuming topic: " + topic);
        System.out.println("Max messages to read: " + maxMessages);

        int count = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Thay vì dùng subscribe (cần Consumer Group Coordinator), ta dùng assign để đọc trực tiếp
            java.util.List<org.apache.kafka.common.PartitionInfo> partitions = consumer.partitionsFor(topic);
            java.util.List<org.apache.kafka.common.TopicPartition> topicPartitions = new java.util.ArrayList<>();
            for (org.apache.kafka.common.PartitionInfo p : partitions) {
                topicPartitions.add(new org.apache.kafka.common.TopicPartition(topic, p.partition()));
            }
            consumer.assign(topicPartitions);
            // Đọc từ CUỐI (latest - N) thay vì từ đầu để tránh bị treo nếu topic có quá nhiều dữ liệu cũ
            java.util.Map<org.apache.kafka.common.TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            for (org.apache.kafka.common.TopicPartition tp : topicPartitions) {
                long endOffset = endOffsets.get(tp);
                long startOffset = Math.max(0, endOffset - maxMessages);
                consumer.seek(tp, startOffset);
            }

            System.out.println("Assigned to " + topicPartitions.size() + " partitions. Seeking to end offsets...");
            
            if (!partitions.isEmpty()) {
                org.apache.kafka.common.Node leader = partitions.get(0).leader();
                System.out.println("DEBUG: Leader for partition 0 is advertised as: " + leader.host() + ":" + leader.port());
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
