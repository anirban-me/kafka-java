package producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.ProducerHelpers;

public class ProducerDemoWithCallback {

    private static final String TOPIC = "topic-100";

    private static String getTopic() {
        return ProducerDemoWithCallback.TOPIC;
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());
    }

    public static void main(String[] args) {
        // 1. Create the producer properties
        Properties properties = ProducerHelpers.getDefaultProperties();

        // 2. Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //Create Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                getTopic(), "From ProducerDemoWithCallback");

        // 3. Send messages
        kafkaProducer.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time kafka responds
                if (e == null) {
                    ProducerHelpers.printRecordMetadataFromProducers(recordMetadata, getLogger());
                } else {
                    getLogger().error("Issue while producing", e);
                }
            }
        });

        kafkaProducer.close();
    }

}
