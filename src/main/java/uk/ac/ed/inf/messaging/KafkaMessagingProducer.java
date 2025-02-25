package uk.ac.ed.inf.messaging;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaMessagingProducer is a concrete implementation of the MessagingProducer class
 * that provides functionality for sending messages to an Apache Kafka topic.
 * <p>
 * This class uses the Kafka producer client to send key-value pairs as messages to
 * a configured Kafka topic. It supports both continuous and non-continuous modes
 * of operation, determined by the configuration properties provided.
 */
public class KafkaMessagingProducer extends MessagingProducer {
    public static final String KafkaTopicConfig = "kafka.topic";

    public KafkaMessagingProducer(Properties props){
        super(props);
    }

    private KafkaProducer<String, String> producer;
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
        producer = new KafkaProducer<String, String>(props);
        topic = props.getProperty(KafkaTopicConfig);
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
     *   timeout, or interruption issues.
     * - Optionally repeating the process with a delay in continuous mode or exiting
     *   after a single message in non-continuous mode.
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
    public void run() {
        try {
            while (true) {
                final String key = getNextSymbol();
                final String value = getNextValue();

                producer.send(new ProducerRecord<>(topic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, key, value);
                }).get(5000, TimeUnit.MILLISECONDS);

                if (! continuousMode) {
                    break;
                }

                Thread.sleep(100);
            }

            System.out.println("1 record sent to Kafka");
        } catch (ExecutionException e) {
            System.err.println("execution exc: " + e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.err.println("timeout exc: " + e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        producer.flush();
        producer.close();
    }
}
