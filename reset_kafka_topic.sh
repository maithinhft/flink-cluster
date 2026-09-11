docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic result \
  --delete
  
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --topic result \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1