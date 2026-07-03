# Gera certificados SSL para Kafka (broker + clientes Spring Boot)
# Requer: Java (keytool) + OpenSSL (Git Bash ou WSL)
#
# Uso (PowerShell):
#   cd C:\development\java\kafka-dlq-demo\kafka\scripts
#   .\generate-certs.ps1

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$CertsDir = Join-Path (Split-Path -Parent $ScriptDir) "certs"
$Password = "changeit"
$Validity = 3650

function Require-Command($name) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        throw "Comando '$name' nao encontrado. Instale OpenSSL (Git for Windows) e Java (keytool)."
    }
}

Require-Command "openssl"
Require-Command "keytool"

New-Item -ItemType Directory -Force -Path $CertsDir | Out-Null
Push-Location $CertsDir

try {
    Write-Host "=== 1/5 CA ==="
    if (-not (Test-Path "ca-key.pem")) {
        openssl req -new -x509 -keyout ca-key.pem -out ca-cert.pem -days $Validity -nodes `
            -subj "/CN=Kafka-Demo-CA/O=Demo/L=SP/ST=SP/C=BR"
    }

    Write-Host "=== 2/5 Broker keystore/truststore ==="
    Remove-Item -Force -ErrorAction SilentlyContinue broker-key.pem, broker-req.pem, broker-cert.pem, broker.p12, kafka.server.keystore.jks
    openssl req -new -newkey rsa:2048 -keyout broker-key.pem -out broker-req.pem -nodes `
        -subj "/CN=localhost/O=Demo/L=SP/ST=SP/C=BR" `
        -addext "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1"
    openssl x509 -req -in broker-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial `
        -out broker-cert.pem -days $Validity -copy_extensions copy

    openssl pkcs12 -export -in broker-cert.pem -inkey broker-key.pem -chain -CAfile ca-cert.pem `
        -name localhost -out broker.p12 -password pass:$Password
    keytool -importkeystore -deststorepass $Password -destkeypass $Password `
        -destkeystore kafka.server.keystore.jks -srckeystore broker.p12 -srcstoretype PKCS12 `
        -srcstorepass $Password -noprompt

    Remove-Item -Force -ErrorAction SilentlyContinue kafka.server.truststore.jks
        keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert.pem `
            -storepass $Password -noprompt

    Write-Host "=== 3/5 Client keystore/truststore (microservicos) ==="
    if (-not (Test-Path "client-key.pem")) {
        openssl req -new -newkey rsa:2048 -keyout client-key.pem -out client-req.pem -nodes `
            -subj "/CN=kafka-client/O=Demo/L=SP/ST=SP/C=BR"
        openssl x509 -req -in client-req.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial `
            -out client-cert.pem -days $Validity
    }

    if (-not (Test-Path "client.keystore.jks")) {
        openssl pkcs12 -export -in client-cert.pem -inkey client-key.pem -chain -CAfile ca-cert.pem `
            -name kafka-client -out client.p12 -password pass:$Password
        keytool -importkeystore -deststorepass $Password -destkeypass $Password `
            -destkeystore client.keystore.jks -srckeystore client.p12 -srcstoretype PKCS12 `
            -srcstorepass $Password -noprompt
    }

    if (-not (Test-Path "client.truststore.jks")) {
        keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert.pem `
            -storepass $Password -noprompt
    }

    Write-Host "=== 4/5 Credenciais Confluent + client-ssl.properties ==="
    "changeit" | Set-Content -NoNewline keystore_creds
    "changeit" | Set-Content -NoNewline key_creds
    "changeit" | Set-Content -NoNewline truststore_creds

    @"
security.protocol=SSL
ssl.truststore.location=/etc/kafka/secrets/client.truststore.jks
ssl.truststore.password=changeit
ssl.keystore.location=/etc/kafka/secrets/client.keystore.jks
ssl.keystore.password=changeit
ssl.key.password=changeit
"@ | Set-Content -Encoding ascii client-ssl.properties

    Write-Host "=== 5/5 Limpeza temporarios ==="
    Remove-Item -Force -ErrorAction SilentlyContinue broker.p12, client.p12, broker-req.pem, client-req.pem

    Write-Host ""
    Write-Host "Certificados gerados em: $CertsDir"
    Get-ChildItem *.jks | Format-Table Name, Length
}
finally {
    Pop-Location
}
