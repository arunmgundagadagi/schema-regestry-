package org.example;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        KafkaProducer<String, Person> producer = new KafkaProducer<String, Person>(props);
        Person kenny = new Person(12345, "Arun", "mg", "Arun@mg.com");
        System.out.println("About to send message for Kenny");

        producer.send(new ProducerRecord<String, Person>("mvnavroprod", String.valueOf(kenny.getId()), kenny)
                ,
                (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Produced message to topic: " + metadata.topic() + " | partition: " + metadata.partition() + " | offset: " + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
        System.out.println("Message send operation initiated for Kenny");
        Person terry = new Person(23444, "Kiran", "Cox", "Kiran@mg.com");
        producer.send(new ProducerRecord<String, Person>("mvnavroprod",  String.valueOf(terry.getId()), terry)
                ,
                (metadata, exception) -> {
                    System.out.println("Callback executed");
                    if (exception == null) {
                        System.out.println("Produced message to topic: " + metadata.topic() + " | partition: " + metadata.partition() + " | offset: " + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
        producer.flush();
        producer.close();
    }}