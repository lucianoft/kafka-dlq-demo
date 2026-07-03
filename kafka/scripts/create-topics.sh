#!/usr/bin/env bash
# Cria topicos via docker exec (SSL localhost dentro do broker)
set -euo pipefail

echo "Aguardando broker..."
sleep 10

topics=("orders-events" "orders-events-dlq")

for topic in "${topics[@]}"; do
  echo "Criando topico: $topic"
  docker exec kafka-broker kafka-topics \
    --bootstrap-server localhost:9093 \
    --command-config /etc/kafka/secrets/client-ssl.properties \
    --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done

echo "Topicos prontos."
