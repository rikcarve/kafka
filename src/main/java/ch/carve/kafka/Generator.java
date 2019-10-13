package ch.carve.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Generator {

    public static void main(String[] args) {
        var producer = createProducer();
        producer.send(new ProducerRecord<String, Integer>("input", "a", 2));
        producer.send(new ProducerRecord<String, Integer>("input", "b", 4));
        producer.send(new ProducerRecord<String, Integer>("input", "a", 1));
        producer.send(new ProducerRecord<String, Integer>("input", "b", 2));
        producer.send(new ProducerRecord<String, Integer>("input", "a", 2));
        producer.flush();
    }

    public static Producer<String, Integer> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sender");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
