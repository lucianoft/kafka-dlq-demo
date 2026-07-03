#!/usr/bin/env bash
# Sobe Kafka + cria topicos (Linux / WSL / Git Bash)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

if [[ ! -f kafka/certs/kafka.server.keystore.jks ]]; then
  echo "Certificados nao encontrados. Rode: ./kafka/scripts/generate-certs.sh"
  exit 1
fi

docker compose up -d --remove-orphans
"$SCRIPT_DIR/create-topics.sh"

echo ""
echo "Kafka SSL:       localhost:9093"
echo "Schema Registry: http://localhost:8085"
echo "Kafka UI:        http://localhost:8090"
