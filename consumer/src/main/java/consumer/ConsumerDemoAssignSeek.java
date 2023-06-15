package consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.ConsumerHelpers;

public class ConsumerDemoAssignSeek {

    private static final String TOPIC = "topic-100";

    private static String getTopic() {
        return ConsumerDemoAssignSeek.TOPIC;
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConsumerDemo.class.getName());
    }

    public static void main(String[] args) {
        // 1. Create the consumer properties
        Properties properties = ConsumerHelpers.getDefaultProperties(null);

        // 2. Create the consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // 3. Assign a partition and offset to read from
        TopicPartition topicPartition = new TopicPartition(getTopic(), 0);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        long offset = 0;

        // 4. Seek for the data from the assigned parameters
        kafkaConsumer.seek(topicPartition, offset);

        int messagesToReadCount = 5;
        int messagesRead = 0;

        // 4. Poll for new data
        while (messagesRead <= messagesToReadCount) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                ConsumerHelpers.printRecordFromConsumers(record, getLogger());

                messagesRead++;
            }
        }

    }

}
