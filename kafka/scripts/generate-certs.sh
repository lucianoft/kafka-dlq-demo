#!/usr/bin/env bash
# Gera certificados SSL para Kafka (broker + clientes Spring Boot)
# Requer: openssl + keytool (JDK)
#
# Uso:
#   cd kafka-dlq-demo/kafka/scripts
#   chmod +x generate-certs.sh
#   ./generate-certs.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="$(cd "$SCRIPT_DIR/../certs" && pwd)"
PASSWORD="changeit"
VALIDITY=3650

cd "$CERTS_DIR"

command -v openssl >/dev/null
command -v keytool >/dev/null

echo "=== 1/5 CA ==="
if [[ ! -f ca-key.pem ]]; then
  openssl req -new -x509 -keyout ca-key.pem -out ca-cert.pem -days "$VALIDITY" -nodes \
    -subj "/CN=Kafka-Demo-CA/O=Demo/L=SP/ST=SP/C=BR"
fi

echo "=== 2/5 Broker keystore/truststore ==="
rm -f broker-key.pem broker-req.pem broker-cert.pem broker.p12 kafka.server.keystore.jks
openssl req -new -newkey rsa:2048 -keyout broker-key.pem -out broker-req.pem -nodes \
  -subj "/CN=localhost/O=Demo/L=SP/ST=SP/C=BR" \
  -addext "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1"
openssl x509 -req -in broker-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
  -out broker-cert.pem -days "$VALIDITY" -copy_extensions copy

openssl pkcs12 -export -in broker-cert.pem -inkey broker-key.pem -chain -CAfile ca-cert.pem \
  -name localhost -out broker.p12 -password pass:"$PASSWORD"
keytool -importkeystore -deststorepass "$PASSWORD" -destkeypass "$PASSWORD" \
  -destkeystore kafka.server.keystore.jks -srckeystore broker.p12 -srcstoretype PKCS12 \
  -srcstorepass "$PASSWORD" -noprompt

rm -f kafka.server.truststore.jks
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert.pem \
  -storepass "$PASSWORD" -noprompt

echo "=== 3/5 Client keystore/truststore (microservicos) ==="
if [[ ! -f client-key.pem ]]; then
  openssl req -new -newkey rsa:2048 -keyout client-key.pem -out client-req.pem -nodes \
    -subj "/CN=kafka-client/O=Demo/L=SP/ST=SP/C=BR"
  openssl x509 -req -in client-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial \
    -out client-cert.pem -days "$VALIDITY"
fi

if [[ ! -f client.keystore.jks ]]; then
  openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -chain -CAfile ca-cert.pem \
    -name kafka-client -out client.p12 -password pass:"$PASSWORD"
  keytool -importkeystore -deststorepass "$PASSWORD" -destkeypass "$PASSWORD" \
    -destkeystore client.keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 \
    -srcstorepass "$PASSWORD" -noprompt
fi

if [[ ! -f client.truststore.jks ]]; then
  keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert.pem \
    -storepass "$PASSWORD" -noprompt
fi

echo "=== 4/5 Credenciais Confluent + client-ssl.properties ==="
printf 'changeit' > keystore_creds
printf 'changeit' > key_creds
printf 'changeit' > truststore_creds

cat > client-ssl.properties <<'EOF'
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/client.truststore.jks
ssl.truststore.password=changeit
ssl.keystore.location=/etc/kafka/secrets/client.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
EOF

echo "=== 5/5 Limpeza temporarios ==="
rm -f broker.p12 client.p12 broker-req.pem client-req.pem

echo ""
echo "Certificados gerados em: $CERTS_DIR"
ls -la *.jks
