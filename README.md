# Kafka DLQ Demo — 3 microserviços + SSL

[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.1-6DB33F?logo=springboot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Avro](https://img.shields.io/badge/Avro-Schema%20Registry-0078D4)](https://docs.confluent.io/platform/current/schema-registry/index.html)
[![SSL](https://img.shields.io/badge/Kafka-SSL%20enabled-blue)](https://kafka.apache.org/documentation/#security_ssl)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Maven](https://img.shields.io/badge/Maven-multi--module-C71A36?logo=apachemaven&logoColor=white)](https://maven.apache.org/)

Demo de **Dead Letter Queue (DLQ)** com Apache Kafka em SSL, **serialização Avro** (Confluent Schema Registry) e três microserviços Spring Boot: um produtor REST, um processador de eventos e um worker dedicado à fila de mensagens com falha.

> **DLQ (Dead Letter Queue)** é um tópico separado onde mensagens que falharam no processamento são enviadas para análise, retry ou descarte — sem bloquear o consumo do tópico principal.

Arquitetura de referência com **produtor**, **processador** e **worker DLQ** separados, pronta para clonar e experimentar localmente.

## Topics sugeridos (GitHub)

Ao publicar o repositório, adicione estes [topics](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/classifying-your-repository-with-topics) na página do repo (**Settings → General → Topics**):

`kafka` `apache-kafka` `dlq` `dead-letter-queue` `spring-boot` `spring-kafka` `avro` `schema-registry` `confluent` `microservices` `java` `docker` `docker-compose` `ssl` `event-driven` `messaging`

**Descrição curta sugerida** (campo *About* do repositório):

> Demo de DLQ com Kafka SSL, Avro + Schema Registry e 3 microserviços Spring Boot.

```
                    orders-events
order-api  ──────────────────────────►  order-processor
(producer)                              (consumer)
                                              │ falha
                                              ▼
                                        orders-events-dlq
                                              │
                                              ▼
                                        order-dlq-worker
                                        (ack + reenvia se falhar)
```

## Microserviços

| Serviço | Porta | Papel |
|---------|-------|-------|
| **order-api** | 8081 | REST → publica em `orders-events` |
| **order-processor** | 8082 | Consome principal; falha → DLQ + ack |
| **order-dlq-worker** | 8083 | Consome DLQ; falha → **ack + reenvio mesma DLQ** |

## Tópicos Kafka

| Tópico | Papel |
|--------|-------|
| `orders-events` | Fila principal de pedidos |
| `orders-events-dlq` | Dead Letter Queue (mensagens com falha no processamento) |

Os nomes usam **hífen** (sem `.` ou `_`) para evitar warnings de métricas do Kafka. As constantes ficam centralizadas em `KafkaTopics` (`kafka-common`).

## Serialização Avro

Mensagens usam **Avro** com **Confluent Schema Registry**. O schema `OrderMessage` fica em `kafka-common/src/main/avro/order-message.avsc` e a classe Java é gerada no build (`avro-maven-plugin`).

| Item | Detalhe |
|------|---------|
| Schema | `order-message.avsc` → `com.demo.kafka.common.avro.OrderMessage` |
| Serializers | `KafkaAvroSerializer` / `KafkaAvroDeserializer` (Confluent) |
| Schema Registry | http://localhost:8085 |
| Factory | `OrderMessages.create(...)` para montar pedidos na API |

O Schema Registry persiste os schemas no Kafka via listener interno SSL (`kafka:9094`). Os microserviços registram/consultam schemas em `app.kafka.schema-registry-url`.

## Infraestrutura local

| Componente | Endereço | Quem usa |
|------------|----------|----------|
| Kafka (SSL) | `localhost:9093` | Microserviços Spring Boot no host |
| Kafka (SSL interno) | `kafka:9094` | Kafka UI, Schema Registry |
| Schema Registry | http://localhost:8085 | Microserviços (Avro) + Kafka UI |
| Kafka UI | http://localhost:8090 | Navegador |

O broker expõe **dois listeners SSL**:

- **`9093`** — apps no host/WSL conectam em `localhost:9093`
- **`9094`** — containers na rede Docker conectam em `kafka:9094` (Kafka UI)

Certificados incluem SAN `localhost`, `kafka` e `127.0.0.1`. O script `generate-certs` usa `-copy_extensions copy` para garantir que o SAN vá para o certificado final.

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
cd kafka-dlq-demo/kafka/scripts
.\generate-certs.ps1
```

**Linux / WSL:**

```bash
cd kafka-dlq-demo/kafka/scripts
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
cd kafka-dlq-demo/kafka/scripts
.\start-stack.ps1
```

**Linux / WSL:**

```bash
cd kafka-dlq-demo/kafka/scripts
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
| Schema Registry | http://localhost:8085 |
| Kafka UI | http://localhost:8090 |

> **Kafka UI offline?** O UI conecta ao broker pela rede Docker (`kafka:9094`). Se o cluster aparecer offline, regenere os certificados (o SAN precisa incluir `kafka`) e recrie os containers:
>
> ```bash
> ./kafka/scripts/generate-certs.sh   # ou generate-certs.ps1
> docker compose up -d --force-recreate kafka kafka-ui
> ```

> **Tópicos antigos?** Se você já tinha `orders.events` / `orders.events.dlq` no cluster, delete-os no Kafka UI ou recrie o ambiente (`docker compose down -v`) antes de rodar `create-topics`.

---

## 3. Compilar e rodar os 3 microserviços

Na raiz do projeto (3 terminais):

```bash
cd kafka-dlq-demo

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
$env:KAFKA_CERTS_DIR = "C:\caminho\para\kafka-dlq-demo\kafka\certs"
```

---

## 4. Testar fluxos

### Sucesso (processamento normal)

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Joao","amount":99.90,"failProcessing":false,"failDlq":false}'
```

### Falha no processor → vai para DLQ

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Maria","amount":50.00,"failProcessing":true,"failDlq":false}'
```

- `order-processor` falha → publica na DLQ → **ack** no tópico principal

### Falha no DLQ worker → ack + reenvio mesma DLQ

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Pedro","amount":30.00,"failProcessing":true,"failDlq":true}'
```

- Processor envia para DLQ
- DLQ worker tenta reprocessar → falha → **ack** → republica em `orders-events-dlq` com header `x-retry-count` incrementado

### Recuperação na DLQ

Envie com `failProcessing:true` (vai para DLQ) e `failDlq:false`:

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Ana","amount":10.00,"failProcessing":true,"failDlq":false}'
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
├── pom.xml                    # parent Maven (+ repo Confluent)
├── docker-compose.yaml        # Kafka SSL + Schema Registry + Kafka UI
├── kafka/
│   ├── scripts/
│   │   ├── generate-certs.ps1 / .sh
│   │   ├── create-topics.ps1 / .sh
│   │   └── start-stack.ps1 / .sh
│   └── certs/                 # gerados (nao commitar)
├── kafka-common/              # Avro schema + topicos + headers + config SSL/Avro
│   └── src/main/avro/order-message.avsc
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
- Compatibilidade de schema Avro (BACKWARD / FULL) em evoluções de contrato
- mTLS com certificados distintos por microserviço
- Kubernetes + Strimzi ou Confluent Cloud
