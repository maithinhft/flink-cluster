package generator.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TopicDiagnosticApp {

    public static void main(String[] args) {
        String topic = "events";
        for (int i = 0; i < args.length; i++) {
            if ("--topic".equals(args[i]) && i + 1 < args.length) {
                topic = args[i + 1];
            }
        }

        String bootstrapServers = EnvLoader.get("SERVER_IP", "127.0.0.1") + ":" + EnvLoader.get("KAFKA_PORT", "9092");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "diag-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        System.out.println("Connecting to Kafka: " + bootstrapServers);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos == null) {
                System.out.println("Topic " + topic + " not found or no partitions.");
                return;
            }

            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo info : partitionInfos) {
                topicPartitions.add(new TopicPartition(info.topic(), info.partition()));
            }

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            System.out.println("Offsets for topic '" + topic + "':");
            for (TopicPartition tp : topicPartitions) {
                long begin = beginningOffsets.getOrDefault(tp, -1L);
                long end = endOffsets.getOrDefault(tp, -1L);
                System.out.println("Partition: " + tp.partition() + " | Earliest: " + begin + " | Latest: " + end + " | Available Messages: " + (end - begin));
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
