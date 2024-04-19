package utility;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerHelpers {

    private static final String LOCALHOST = "127.0.0.1:9092";

    public static Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST);

        //Kafka turns everything into bits. So, it should know the serialization and deserialization types.
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return properties;
    }

    public static void printRecordMetadataFromProducers(RecordMetadata recordMetadata, Logger logger) {
        logger.info("Received \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                recordMetadata.timestamp());
    }
}


