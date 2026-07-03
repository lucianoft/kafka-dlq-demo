# Kafka DLQ Demo — 3 microserviços + SSL

[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.1-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![SSL](https://img.shields.io/badge/Kafka-SSL%20enabled-blue)](https://kafka.apache.org/documentation/#security_ssl)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Maven](https://img.shields.io/badge/Maven-multi--module-C71A36?logo=apachemaven&logoColor=white)](https://maven.apache.org/)

Demo de **Dead Letter Queue (DLQ)** com Apache Kafka em SSL e três microserviços Spring Boot: um produtor REST, um processador de eventos e um worker dedicado à fila de mensagens com falha.

> **DLQ (Dead Letter Queue)** é um tópico separado onde mensagens que falharam no processamento são enviadas para análise, retry ou descarte — sem bloquear o consumo do tópico principal.

Arquitetura de referência com **produtor**, **processador** e **worker DLQ** separados, pronta para clonar e experimentar localmente.

## Topics sugeridos (GitHub)

Ao publicar o repositório, adicione estes [topics](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/classifying-your-repository-with-topics) na página do repo (**Settings → General → Topics**):

`kafka` `apache-kafka` `dlq` `dead-letter-queue` `spring-boot` `spring-kafka` `microservices` `java` `docker` `docker-compose` `ssl` `event-driven` `messaging`

**Descrição curta sugerida** (campo *About* do repositório):

> Demo de Dead Letter Queue com Kafka SSL e 3 microserviços Spring Boot (producer, processor, DLQ worker).

```
                    orders.events
order-api  ──────────────────────────►  order-processor
(producer)                              (consumer)
                                              │ falha
                                              ▼
                                        orders.events.dlq
                                              │
                                              ▼
                                        order-dlq-worker
                                        (ack + reenvia se falhar)
```

## Microserviços

| Serviço | Porta | Papel |
|---------|-------|-------|
| **order-api** | 8081 | REST → publica em `orders.events` |
| **order-processor** | 8082 | Consome principal; falha → DLQ + ack |
| **order-dlq-worker** | 8083 | Consome DLQ; falha → **ack + reenvio mesma DLQ** |

## Pré-requisitos

- Docker + Docker Compose
- Java 21 + Maven
- OpenSSL (Git for Windows ou WSL)

---

## 1. Gerar certificados SSL

| Script | Plataforma |
|--------|------------|
| `generate-certs.ps1` | Windows (PowerShell) |
| `generate-certs.sh` | Linux / WSL / Git Bash |

**Windows (PowerShell):**

```powershell
cd C:\development\java\kafka-dlq-demo\kafka\scripts
.\generate-certs.ps1
```

**Linux / WSL:**

```bash
cd /mnt/c/development/java/kafka-dlq-demo/kafka/scripts
sed -i 's/\r$//' *.sh   # se clonou no Windows
chmod +x *.sh
./generate-certs.sh
```

---

## 2. Subir Kafka (SSL)

| Script | Plataforma |
|--------|------------|
| `start-stack.ps1` | Windows |
| `start-stack.sh` | Linux / WSL |

**Windows:**

```powershell
cd C:\development\java\kafka-dlq-demo\kafka\scripts
.\start-stack.ps1
```

**Linux / WSL:**

```bash
cd /mnt/c/development/java/kafka-dlq-demo/kafka/scripts
./start-stack.sh
```

Ou manualmente:

```bash
cd kafka-dlq-demo
docker compose up -d --remove-orphans
./kafka/scripts/create-topics.sh    # Linux
# ou
.\kafka\scripts\create-topics.ps1   # Windows
```

| Serviço | URL |
|---------|-----|
| Kafka (SSL) | `localhost:9093` |
| Kafka UI | http://localhost:8090 |

Tópicos criados automaticamente:

- `orders.events`
- `orders.events.dlq`

---

## 3. Compilar e rodar os 3 microserviços

Na raiz do projeto (3 terminais):

```bash
cd C:\development\java\kafka-dlq-demo

# Terminal 1
cd order-api && mvn spring-boot:run

# Terminal 2
cd order-processor && mvn spring-boot:run

# Terminal 3
cd order-dlq-worker && mvn spring-boot:run
```

Ou importe o POM pai no STS/Eclipse e rode cada `*Application` como Spring Boot App.

**Certificados:** os apps usam `../kafka/certs` relativo à pasta de cada módulo. Se necessário:

```powershell
$env:KAFKA_CERTS_DIR = "C:\development\java\kafka-dlq-demo\kafka\certs"
```

---

## 4. Testar fluxos

### Sucesso (processamento normal)

```bash
curl -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"customer\":\"Joao\",\"amount\":99.90,\"failProcessing\":false,\"failDlq\":false}"
```

### Falha no processor → vai para DLQ

```bash
curl -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"customer\":\"Maria\",\"amount\":50.00,\"failProcessing\":true,\"failDlq\":false}"
```

- `order-processor` falha → publica na DLQ → **ack** no tópico principal

### Falha no DLQ worker → ack + reenvio mesma DLQ

```bash
curl -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"customer\":\"Pedro\",\"amount\":30.00,\"failProcessing\":true,\"failDlq\":true}"
```

- Processor envia para DLQ
- DLQ worker tenta reprocessar → falha → **ack** → republica em `orders.events.dlq` com header `x-retry-count` incrementado

### Recuperação na DLQ

Envie com `failProcessing:true` (vai para DLQ) e `failDlq:false`:

```bash
curl -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"customer\":\"Ana\",\"amount\":10.00,\"failProcessing\":true,\"failDlq\":false}"
```

DLQ worker reprocessa com sucesso → ack → mensagem sai da fila.

---

## Headers de rastreio (DLQ)

| Header | Descrição |
|--------|-----------|
| `x-retry-count` | Tentativas na DLQ |
| `x-original-topic` | Tópico de origem |
| `x-error-message` | Último erro |
| `x-failed-at` | Timestamp ISO |

---

## Estrutura do projeto

```
kafka-dlq-demo/
├── pom.xml                    # parent Maven
├── docker-compose.yaml
├── kafka/
│   ├── scripts/
│   │   ├── generate-certs.ps1 / .sh
│   │   ├── create-topics.ps1 / .sh
│   │   └── start-stack.ps1 / .sh
│   └── certs/                 # gerados (nao commitar)
├── kafka-common/              # DTO + topicos + headers
├── order-api/                 # microservico 1
├── order-processor/           # microservico 2
└── order-dlq-worker/          # microservico 3
```

---

## Parar

```bash
docker compose down
# com volumes:
docker compose down -v
```

---

## Produção — proximos passos

- Limitar retries (`x-retry-count` max → parking lot topic)
- Métricas Prometheus por serviço
- Schema Registry (Avro) se necessário
- mTLS com certificados distintos por microserviço
- Kubernetes + Strimzi ou Confluent Cloud
