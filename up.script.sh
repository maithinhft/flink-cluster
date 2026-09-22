#!/bin/bash

set -e

echo "Starting Docker Compose..."
docker compose up -d --remove-orphans

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

echo "Exporting Kerberos keytab and config to ./security for host clients..."
mkdir -p ./security
docker compose cp kdc:/var/lib/secret/client.keytab ./security/client.keytab 2>/dev/null || true
docker compose cp kdc:/var/lib/secret/krb5.conf ./security/krb5.conf 2>/dev/null || true

echo "Running setup kafka topic"
./scripts/create-kafka-topics.sh
echo "Running setup kafka connector"
./scripts/register-all-connectors.sh

echo "Running setup schema topic"
(
    cd ./data-generator
    mvn clean package
    mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/crm"
    mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/ecommerce"
    mvn exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/payment"
)

echo "Setup completed successfully!"
