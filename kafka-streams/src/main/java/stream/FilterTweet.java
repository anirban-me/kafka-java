package stream;

import com.google.gson.JsonParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import utility.StreamsHelper;

import java.util.Properties;

public class FilterTweet {
    private static final String INPUT_TOPIC = "topic-twitter-es";
    private static final String OUTPUT_TOPIC = "topic-twitter-es-filtered";

    private static final int TWEET_FOLLOWER_THRESHOLD = 500;

    private static String getInputTopic() {
        return FilterTweet.INPUT_TOPIC;
    }

    private static String getOutputTopic() {
        return FilterTweet.OUTPUT_TOPIC;
    }

    private static Integer extractUserFollowersFromTweet(String jsonTweet) {
        try {
            return JsonParser.parseString(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception e) {
            return 0;
        }
    }


    public static void main(String[] args) {
        // create properties
        Properties properties = StreamsHelper.getDefaultStreamProperties();

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // get from input topic -> filter -> put to output topic
        KStream<String, String> inputTopic = streamsBuilder.stream(getInputTopic());
        KStream<String, String> filteredStream = inputTopic.filter(
                (key, value) -> extractUserFollowersFromTweet(value) > TWEET_FOLLOWER_THRESHOLD);
        filteredStream.to(getOutputTopic());

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start streams
        kafkaStreams.start();
    }
}
