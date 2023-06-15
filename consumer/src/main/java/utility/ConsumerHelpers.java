package utility;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Properties;

public class ConsumerHelpers {

    public static Properties getDefaultProperties(String group) {
        String bootStrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        if (Objects.nonNull(group)) {
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        }
        return properties;
    }

    public static void printRecordFromConsumers(ConsumerRecord<String, String> record, Logger logger) {
        logger.info("Consumed \n Value: {} \n Key: {} \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
                record.value(),
                record.key(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp());
    }
}


