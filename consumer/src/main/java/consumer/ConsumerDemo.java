package consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.ConsumerHelpers;

public class ConsumerDemo {

    private static final String TOPIC = "topic-100";
    private static final String GROUP_ID = "group-1";

    private static String getTopic() {
        return ConsumerDemo.TOPIC;
    }

    private static String getGroup() {
        return ConsumerDemo.GROUP_ID;
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConsumerDemo.class.getName());
    }

    public static void main(String[] args) {
        // 1. Create the consumer properties
        Properties properties = ConsumerHelpers.getDefaultProperties(getGroup());

        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10000000");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "7000");
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "6999");

        // 2. Create the consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 3. Subscribe to a topic
        kafkaConsumer.subscribe(Collections.singleton(getTopic()));

        // 4. Poll for new data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                ConsumerHelpers.printRecordFromConsumers(record, getLogger());
            }

            kafkaConsumer.commitAsync();
        }
    }

}
