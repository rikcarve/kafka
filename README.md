# kafka
Kafka (Streams) playground

### Some kafka console commands
```bash
kafka-topics.sh --zookeeper localhost:2181 --create --topic input --partitions 1 --replication-factor 1
kafka-topics.sh --zookeeper localhost:2181 --create --topic output --partitions 1 --replication-factor 1
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output --from-beginning --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
kafka-topics.sh --zookeeper localhost:2181 --list
kafka-topics.sh --zookeeper localhost:2181 --alter --topic input --config retention.ms=60000
```
