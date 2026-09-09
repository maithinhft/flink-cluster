docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic rule_definitions \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic debezium_heartbeat \
  --create \
  --if-not-exists \
  --partitions 1 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic events_crm \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic events_ecommerce \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic events_payment \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic schema_registry \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic result \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic dlq \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1