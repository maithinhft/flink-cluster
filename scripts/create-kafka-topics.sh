docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server kafka:29092 \
  --topic realtime_core.public.rule_definitions \
  --partitions 6 \
  --replication-factor 1

docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --bootstrap-server kafka:29092 \
  --topic __debezium-heartbeat.realtime_core \
  --partitions 1 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic events.crm \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic events.ecommerce \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic events.payment \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --topic schema_registry \
  --partitions 6 \
  --replication-factor 1