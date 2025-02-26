package uk.ac.ed.inf.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaMessagingConsumer is a concrete implementation of the MessagingConsumer class
 * that provides functionality for sending messages to an Apache Kafka topic.
 * <p>
 * This class uses the Kafka producer client to send key-value pairs as messages to
 * a configured Kafka topic. It supports both continuous and non-continuous modes
 * of operation, determined by the configuration properties provided.
 */
public class KafkaMessagingConsumer extends MessagingConsumer {
    public static final String KafkaTopicConfig = "kafka.topic";


    public KafkaMessagingConsumer(Properties props) {
        super(props);
    }

    private KafkaConsumer<String, String> consumer;
    private String topic;

    /**
     * Initializes the Kafka producer and sets the target topic based on the provided properties.
     * This method is responsible for creating a `KafkaProducer` instance using the configuration
     * properties and retrieving the topic name from the properties.
     * <p>
     * The producer is configured to send messages to the Kafka topic specified by the
     * property `KafkaTopicConfig`. The required configurations for the Kafka producer,
     * such as brokers and serialization settings, must be included in the `props` object.
     * <p>
     * Throws a `RuntimeException` if the properties necessary for initializing the producer
     * or the topic are missing or invalid.
     */
    @Override
    public void init() {
        topic = props.getProperty(KafkaTopicConfig);
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));


    }

    /**
     * Continuously sends messages to a Kafka topic according to the current configuration.
     * This method operates in either continuous or non-continuous mode based on the
     * `continuousMode` flag. It retrieves key-value data pairs using `getNextSymbol`
     * and `getNextValue`, and sends them as records to the configured Kafka topic.
     * <p>
     * The operation involves:
     * - Creating and sending `ProducerRecord` objects with the topic, key, and value.
     * - Handling potential exceptions during message production, such as execution,
     * timeout, or interruption issues.
     * - Optionally repeating the process with a delay in continuous mode or exiting
     * after a single message in non-continuous mode.
     * <p>
     * After execution, the Kafka producer is flushed and closed to ensure all produced
     * records are properly sent before shutting down.
     * <p>
     * Exceptions such as `ExecutionException`, `TimeoutException`, or `InterruptedException`
     * are handled and logged appropriately during the process.
     * <p>
     * Throws a `RuntimeException` for unrecoverable issues such as interrupted execution
     * or execution failures that prevent successful message production.
     */
    @Override
    public void run() throws InterruptedException {
        int recordCount = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<String, String> record : records) {
                if (currentSymbolValueMap.containsKey(record.key())) {
                    currentSymbolValueMap.put(record.key(), Double.parseDouble(record.value()));

                    System.out.printf("[%s] %s: %20s %2s %6s %s%n", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
                    recordCount++;
                } else {
                    System.out.printf("Unknown symbol: %s with value: %s encountered%n", record.key(), record.value());
                }
            }

            System.out.println(recordCount + " records received");
            Thread.sleep(500);
        }
    }
}
