# Cria topicos via docker exec (SSL localhost dentro do broker)
$ErrorActionPreference = "Stop"

Write-Host "Aguardando broker..."
Start-Sleep -Seconds 10

$topics = @("orders.events", "orders.events.dlq")

foreach ($topic in $topics) {
    Write-Host "Criando topico: $topic"
    docker exec kafka-broker kafka-topics `
        --bootstrap-server localhost:9093 `
        --command-config /etc/kafka/secrets/client-ssl.properties `
        --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
}

Write-Host "Topicos prontos."
