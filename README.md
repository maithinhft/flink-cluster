# flink-cluster

## Setup cụm
* Chạy cụm:
```
docker compose up -d
```
* Do cấu hình Kafka không cho phép tự động khởi tạo topics nên cần chạo các topics trong Kafka
```
./scripts/create-kafka-topics.sh
```
* Register postgres connector lên Kafka connect
```
./scripts/register-connector.sh
```
## Các lệnh support
* Build file jar
```
mvn clean package
```
* Submit job trực tiếp thông qua jobmanager
```
docker cp target/flink-example-1.0-SNAPSHOT.jar flink-jobmanager:/tmp/

docker exec -it flink-jobmanager \
    ./bin/flink run \
    -m jobmanager:8081 \
    /tmp/flink-example-1.0-SNAPSHOT.jar

```

* Xem các message của 1 topics trong Kafka
```
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic realtime_core.public.aggregation_definitions \
  --from-beginning
```

# Sinh dữ liệu

* Compile
```
mvn clean package
```

hoặc nếu chỉ compile thư mục test
```
mvn test-compile
```

* Tạo các schema
```
mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/crm"
mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/ecommerce"
mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/payment"
```

* Sinh các Event
```
mvn exec:java -Dexec.mainClass=generator.events.EventGeneratorApp
```

* Sinh các Rule
```
mvn exec:java -Dexec.mainClass=generator.rules.RuleGeneratorApp
```

* Kiểm tra 1 topic trên Kafka
```
mvn exec:java -Dexec.classpathScope=test -Dexec.mainClass="generator.common.KafkaConsumerApp" -Dexec.args="--topic events --max 5"\n
```
**Note:** nên chạy trên server để tránh tình trạng mất gói tin dẫn đến java bị treo