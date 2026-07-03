# Sobe Kafka + cria topicos (Windows PowerShell)
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $ProjectRoot

if (-not (Test-Path "kafka\certs\kafka.server.keystore.jks")) {
    Write-Error "Certificados nao encontrados. Rode: .\kafka\scripts\generate-certs.ps1"
}

docker compose up -d --remove-orphans
& "$PSScriptRoot\create-topics.ps1"

Write-Host ""
Write-Host "Kafka SSL: localhost:9093"
Write-Host "Kafka UI:  http://localhost:8090"
