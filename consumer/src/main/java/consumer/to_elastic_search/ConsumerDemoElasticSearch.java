package consumer.to_elastic_search;

import consumer.ConsumerDemo;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.ConsumerHelpers;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class ConsumerDemoElasticSearch {

    private static final String TOPIC = "topic-twitter-es";
    private static final String GROUP_ID = "tweet-consumer";
    private static final String SEPARATOR = "_";

    private static String getTopic() {
        return ConsumerDemoElasticSearch.TOPIC;
    }

    private static String getGroup() {
        return ConsumerDemoElasticSearch.GROUP_ID;
    }

    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConsumerDemo.class.getName());
    }

    private static String getUniqueRecordId(ConsumerRecord<String, String> record) {
        // Generating this is important to maintain idempotency in case of duplicate records
        // ES will update data with already existing id
        return record.topic() + SEPARATOR
                + record.partition() + SEPARATOR
                + record.offset();
    }

    // Returns a client which connects to ES
    private static RestHighLevelClient getELasticSearchClient() {
        // https://rw0lvh7ltk:2le16iy6hw@kafka-course-anirban-7671364774.eu-central-1.bonsaisearch.net:443
        String hostname = "kafka-course-anirban-7671364774.eu-central-1.bonsaisearch.net";
        String username = "rw0lvh7ltk";
        String password = "2le16iy6hw";

        // Not needed for a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(restClientBuilder);
    }

    private static IndexRequest getIndexRequest(ConsumerRecord<String, String> record) {
        String indexName = "index-demo"; // Create this index at the ES cluster (lecture 72)
        String documentName = "document";

        return new IndexRequest(indexName, documentName, getUniqueRecordId(record))
                .source(record.value(), XContentType.JSON);
    }

    private static BulkResponse insertDataToElasticSearchInBulk(BulkRequest bulkRequest) throws IOException {
        RestHighLevelClient restHighLevelClient = getELasticSearchClient();
        BulkResponse response = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        restHighLevelClient.close();
        return response;
    }

    public static void main(String[] args) throws IOException {
        // 1. Create the consumer properties
        Properties properties = ConsumerHelpers.getDefaultProperties(getGroup());
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10000000"); // max data per partition
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10"); // get only 10 records in a poll

        Logger logger = getLogger();

        // 2. Create the consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 3. Subscribe to a topic
        kafkaConsumer.subscribe(Collections.singleton(getTopic()));

        // 4. Poll for new data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            int recordCount = consumerRecords.count();
            logger.info("Received: " + recordCount + " at " + new Date());

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : consumerRecords) {
                bulkRequest.add(getIndexRequest(record));
//                ConsumerHelpers.printRecordFromConsumers(record, logger);
            }

            if (recordCount > 0) {
                insertDataToElasticSearchInBulk(bulkRequest);
                logger.info("Committing the offsets");
                kafkaConsumer.commitSync(); // commitAsync is also an option
            }
        }
    }
}
