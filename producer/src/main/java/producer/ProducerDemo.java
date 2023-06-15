package producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import utility.ProducerHelpers;

public class ProducerDemo {

    private static final String TOPIC = "test-topic";

    private static String getTopic() {
        return ProducerDemo.TOPIC;
    }

    public static void main(String[] args) {
        // 1. Create the producer properties
        Properties properties = ProducerHelpers.getDefaultProperties();

        // 2. Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //Create Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                getTopic(), "From ProducerDemo");

        // 3. Send messages
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();
    }

}
