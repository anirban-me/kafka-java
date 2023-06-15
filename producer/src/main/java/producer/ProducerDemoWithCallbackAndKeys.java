package producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.ProducerHelpers;

public class ProducerDemoWithCallbackAndKeys {

    private static final String TOPIC = "topic-100";
    private static final int SLEEP_AFTER = 5;

    private static String getTopic() {
        return ProducerDemoWithCallbackAndKeys.TOPIC;
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger(ProducerDemoWithCallbackAndKeys.class);
    }

    private static void addPropertiesForSafeProducer(Properties properties) {
        // Safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Only setting the above value will set the 3 below by default. Not needed to set explicitly.
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5)); // Kafka 2.0
    }

    private static void addPropertiesForHighThroughput(Properties properties) {
        // High throughput settings. The consumer knows how to decompress messages and read batches.
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10"); // in ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB
    }

    public static void main(String[] args) throws InterruptedException {
        final Logger logger = getLogger();

        // 1. Create the producer properties
        Properties properties = ProducerHelpers.getDefaultProperties();
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10000000");
        addPropertiesForSafeProducer(properties);
        addPropertiesForHighThroughput(properties);

        // 2. Create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //Create Producer Record(s)
        for (int i = 0; i <= 100000; i++) {
            String topic = getTopic();
            String value = "message number " + i;
            String key = "key-value" + i;

            if (i % SLEEP_AFTER == 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw e;
                }
            }

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            // 3. Send messages
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    // executes every time kafka responds
                    if (exception == null) {
                        ProducerHelpers.printRecordMetadataFromProducers(recordMetadata, logger);

                    } else {
                        logger.error("Issue while producing", exception);
                    }
                }
            });
        }

        kafkaProducer.close();
    }

}
