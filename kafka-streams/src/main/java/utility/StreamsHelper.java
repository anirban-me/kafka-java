package utility;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class StreamsHelper {
    public static Properties getDefaultStreamProperties() {
        String bootStrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams"); // like consumer config group

        // Strings as keys and values. Used for serialization and de-serialization.
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }
}
