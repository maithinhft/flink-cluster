docker compose exec kafka-plain /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-plain:29092 \
  --command-config /etc/kafka/secrets/client.properties \
  --topic result \
  --delete
  
docker compose exec kafka-plain /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-plain:29092 \
  --command-config /etc/kafka/secrets/client.properties \
  --topic result \
  --create \
  --if-not-exists \
  --partitions 6 \
  --replication-factor 1