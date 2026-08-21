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
  --create \
  --topic events \
  --partitions 6 \
  --replication-factor 1

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --topic registry_schema \
  --partitions 6 \
  --replication-factor 1