docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic realtime_core.public.rule_definitions \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic __debezium-heartbeat.realtime_core \
  --create \
  --if-not-exists \
  --partitions 1 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic events.crm \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic events.ecommerce \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic events.payment \
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