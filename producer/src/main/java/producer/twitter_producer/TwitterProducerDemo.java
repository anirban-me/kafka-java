package producer.twitter_producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.ProducerHelpers;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducerDemo {

    private static final String TOPIC = "topic-twitter-es";

    // Generate from https://developer.twitter.com/en/portal/apps/19947594/keys
    private static final String CONSUMER_KEY = "RTRCxpQjA7iiiDTvXEhAqrD0h";
    private static final String CONSUMER_SECRET = "3wDkspX6qsonPaNBolbLjz8d5JXyj43B9j3JNRTFdwpbZlLDRJ";
    private static final String TOKEN = "937426698626142208-VakQRJEyf7YimKB35lnNZUXGpfMYBTE";
    private static final String SECRET = "wRDBxwJJIVXpKquRyctB2bDYRXo8qfnXD4JF6UZb1GWY3";

    private static Logger getLogger() {
        return LoggerFactory.getLogger(TwitterProducerDemo.class.getName());
    }

    private static String getTopic() {
        return TwitterProducerDemo.TOPIC;
    }

    private static String getConsumerKey() {
        return TwitterProducerDemo.CONSUMER_KEY;
    }

    private static String getConsumerSecret() {
        return TwitterProducerDemo.CONSUMER_SECRET;
    }

    private static String getToken() {
        return TwitterProducerDemo.TOKEN;
    }

    private static String getSecret() {
        return TwitterProducerDemo.SECRET;
    }

    private static KafkaProducer<String, String> getKafkaProducer() {
        Properties properties = ProducerHelpers.getDefaultProperties();
        return new KafkaProducer<>(properties);
    }

    public static void main(String[] args) {
        // See documentation here : https://github.com/twitter/hbc
        TwitterProducerDemo twitterProducerDemo = new TwitterProducerDemo();
        twitterProducerDemo.execute();

    }

    private void execute() {
        // Client puts the message to this queue. Size should be determined by the size of incoming stream.
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(1000);
        List<String> termsToFind = Lists.newArrayList("farmers");

        // Twitter Client
        Client hosebirdClient = createTwitterClient(messageQueue, termsToFind);
        hosebirdClient.connect();

        // Kafka Producer
        KafkaProducer<String, String> kafkaProducer = getKafkaProducer();

        // Loop to send tweets to kafka broker
        while (!hosebirdClient.isDone()) {
            String message = null;

            try {
                message = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (message != null) {
                getLogger().info(message);
                kafkaProducer.send(new ProducerRecord<>(getTopic(), message));
            }
        }
        kafkaProducer.close();
    }

    /*
     * 1. Connect to STREAM_HOST
     * 2. Decides on the terms to track for
     * 3. Setup the OAuth
     * 4. Returns the client
     * */
    private Client createTwitterClient(BlockingQueue<String> messageQueue, List<String> termsToFind) {
        // The client connects to the STREAM_HOST
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(termsToFind); // Terms for tracking

        // Set up the OAuth for Auth
        Authentication hosebirdAuth = new OAuth1(getConsumerKey(), getConsumerSecret(), getToken(), getSecret());

        // Create the client
        return new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(messageQueue))
                .build();
    }


}
