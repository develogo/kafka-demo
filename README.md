# kafka-demo

Stack local de Kafka (KRaft, sem ZooKeeper) + Kafka UI via Docker Compose.

## Subir

```bash
docker compose up -d
```

| Serviço  | Endereço                                     |
| -------- | -------------------------------------------- |
| Kafka    | `localhost:9092` (bootstrap para o host)      |
| Kafka UI | http://localhost:8082                         |

Dentro da rede do Compose, o bootstrap é `kafka:19092`.

> A UI usa a porta `8082` porque `8080`/`8081` já estavam em uso na máquina de desenvolvimento.

## Uso rápido

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic demo --partitions 3 --replication-factor 1

echo "hello-kafka" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic demo

docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic demo --from-beginning --max-messages 1
```

No Git Bash (Windows), prefixe com `MSYS_NO_PATHCONV=1` para evitar a conversão dos caminhos `/opt/...`.

## Derrubar

```bash
docker compose down        # mantém os dados no volume kafka-data
docker compose down -v     # apaga também os dados
```
