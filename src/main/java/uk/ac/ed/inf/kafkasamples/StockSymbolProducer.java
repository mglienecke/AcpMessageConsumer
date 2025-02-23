package uk.ac.ed.inf.kafkasamples;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.utils.Time;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StockSymbolProducer {

        public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        var producer = new StockSymbolProducer();
        producer.process(args[0]);
    }

    // We'll reuse this function to load properties from the Consumer as well
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public final String StockSymbolsConfig = "stock.symbols";
    public final String KafkaTopicConfig = "kafka.topic";
    public final String KafkaContinuousTopicConfig = "send.operation.continuous";

    private void process(String configFileName) throws IOException, InterruptedException {
        Properties kafkaPros = StockSymbolProducer.loadConfig(configFileName);

        var producer = new KafkaProducer<String, String>(kafkaPros);
        String[] symbols = ((String) kafkaPros.get(StockSymbolsConfig)).split(",");

        int recordCount = 0;
        final String topic = kafkaPros.getProperty(KafkaTopicConfig);
        // final String key = String.valueOf(new Random().nextInt(0, Integer.MAX_VALUE));
        final boolean continuousMode = Boolean.parseBoolean(kafkaPros.getProperty(KafkaContinuousTopicConfig));

        try {
            int valueCounter = 1;

            while (true) {
                final String key = symbols[new Random().nextInt(symbols.length)];
                final String value = String.valueOf(valueCounter++);

                producer.send(new ProducerRecord<>(topic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, key, value);
                }).get(1000, TimeUnit.MILLISECONDS);

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
        }

        producer.flush();
        producer.close();
    }
}

