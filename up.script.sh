#!/bin/bash

set -e

echo "Starting Docker Compose..."
docker compose up -d

echo "Waiting for containers to become healthy..."

while true; do
    unhealthy=$(docker compose ps --format json | \
        jq -r 'select(.Health != "healthy" and .Health != "") | .Service' | \
        wc -l)

    if [ "$unhealthy" -eq 0 ]; then
        break
    fi

    echo "Some services are not ready yet..."
    sleep 5
done

echo "All services are ready!"

echo "Running setup kafka topic"
./scripts/create-kafka-topics.sh
echo "Running setup kafka connector"
./scripts/register-connector.sh

echo "Running setup schema topic"
cd ./data-generator
mvn clean package
mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/crm"
mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/ecommerce"
mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/payment"
