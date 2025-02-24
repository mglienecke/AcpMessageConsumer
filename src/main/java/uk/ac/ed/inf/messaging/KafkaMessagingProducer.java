package uk.ac.ed.inf.messaging;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaMessagingProducer extends MessagingProducer {
    public static final String KafkaTopicConfig = "kafka.topic";

    public KafkaMessagingProducer(Properties props){
        super(props);
    }

    private KafkaProducer<String, String> producer;
    private String topic;

    @Override
    public void init() {
        producer = new KafkaProducer<String, String>(props);
        topic = props.getProperty(KafkaTopicConfig);
    }

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
