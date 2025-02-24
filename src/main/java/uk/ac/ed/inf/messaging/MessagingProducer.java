package uk.ac.ed.inf.messaging;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public abstract class MessagingProducer {
    public static final String ContinuousSendConfig = "send.operation.continuous";
    public static final String SendOperationChannelProvider = "send.operation.channel.provider";
    public static final String StockSymbolsConfig = "stock.symbols";


    protected final Properties props;
    protected final boolean continuousMode;

    
    /**
     * Creates and returns a MessagingProducer instance based on the configuration
     * properties specified in the given configuration file. The returned producer
     * will either be a RabbitMqMessagingProducer or a KafkaMessagingProducer,
     * depending on the property value of "send.operation.channel.provider" in the
     * configuration file.
     *
     * @param configFile the path to the configuration file containing the producer's
     *                   properties such as connection details and channel provider type.
     * @return an instance of MessagingProducer, either a RabbitMqMessagingProducer
     *         or a KafkaMessagingProducer, configured based on the specified properties.
     * @throws IOException if there is an error reading the configuration file.
     */
    public static MessagingProducer getMessagingProducer(String configFile) throws IOException {
        Properties props = loadConfig(configFile);
        MessagingProducer producer = null;

        if (props.getProperty(SendOperationChannelProvider).equals("rabbitmq")) {
            producer = new RabbitMqMessagingProducer(props);
        } else {
            producer = new KafkaMessagingProducer(props);
        }

        return producer;
    }

    protected MessagingProducer(Properties props) {
        this.props = props;
        this.continuousMode = Boolean.parseBoolean(props.getProperty(ContinuousSendConfig));
    }

    public abstract void init();
    public abstract void run() throws IOException, InterruptedException, TimeoutException;

    protected String createMessage(String key, String value) {
        return String.format("{\"symbol\":\"%s\",\"value\":\"%s\"}", key, value);
    }

    protected String getNextSymbol() {
        String[] symbols = ((String) props.get(StockSymbolsConfig)).split(",");
        return symbols[new Random().nextInt(symbols.length)];
    }

    private static int valueCounter = 1;

    protected String getNextValue() {
        return String.valueOf(valueCounter++);
    }

    /**
     * Loads the configuration properties from a specified file.
     *
     * @param configFile the path to the configuration file to be loaded
     * @return a {@code Properties} object containing the properties read from the specified file
     * @throws IOException if the specified file does not exist or if an error occurs while reading the file
     */
    private static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
