import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Producer {
    private static final String TOPIC_NAME = "vdt2024";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String dataPath = "..\\input_data\\log_action.csv";

    public static void main(String[] args) {
        // Configure kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
            String line;
            int recordCount = 0;

            while ((line = reader.readLine()) != null) {
                // String[] fields = line.split(",");
                String key = String.valueOf(recordCount);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, line);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println( "Record id: " + record.key() + "sent successfully" +
                                ", topic name: " + metadata.topic() +
                                ", partition: " + metadata.partition() +
                                ", offset: " + metadata.offset());
                    } else {
                        System.out.println("Error while sending record " + record.key() + ": " + exception.getMessage());
                    }
                });
                recordCount++;
                TimeUnit.SECONDS.sleep(1);
            }
            System.out.println(recordCount + " records sent to Kafka successfully.");
        } catch (IOException | InterruptedException e) {
            System.out.println("Source not found or can't connect to Kafka Broker");
        }
    }
}
