package uk.ac.ed.inf.messaging;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;


/**
 * Abstract base class representing a messaging producer. This class provides
 * the common framework and shared functionality for specific messaging producers
 * (e.g., RabbitMQ, Kafka) while leaving the concrete implementations of certain
 * methods to the subclasses.
 * <p>
 * The MessagingConsumer is responsible for producing messages to a messaging system
 * in either continuous or non-continuous mode. Configuration properties and other setup
 * details are provided during the creation of the producer.
 */
public abstract class MessagingConsumer {
    public static final String SendOperationChannelProvider = "send.operation.channel.provider";
    public static final String StockSymbolsConfig = "stock.symbols";

    protected final Properties props;
    protected final HashMap<String, Double> currentSymbolValueMap = new HashMap<>();

    /**
     * Creates and returns a MessagingConsumer instance based on the configuration
     * properties specified in the given configuration file. The returned producer
     * will either be a RabbitMqMessagingConsumer or a KafkaMessagingConsumer,
     * depending on the property value of "send.operation.channel.provider" in the
     * configuration file.
     *
     * @param configFile the path to the configuration file containing the producer's
     *                   properties such as connection details and channel provider type.
     * @return an instance of MessagingConsumer, either a RabbitMqMessagingConsumer
     *         or a KafkaMessagingConsumer, configured based on the specified properties.
     * @throws IOException if there is an error reading the configuration file.
     */
    public static MessagingConsumer getMessagingConsumer(String configFile) throws IOException {
        Properties props = loadConfig(configFile);
        return props.getProperty(SendOperationChannelProvider, "kafka").equals("rabbitmq") ?
                new RabbitMqMessagingConsumer(props) : new KafkaMessagingConsumer(props);
    }

    /**
     * Constructs a MessagingConsumer instance using the provided configuration properties.
     * The properties determine the producer's configuration, including continuous mode
     * and other necessary settings for message production.
     *
     * @param props the configuration properties used for setting up the MessagingConsumer.
     *              The properties may include settings such as messaging provider-specific
     *              configurations and operational mode flags.
     */
    protected MessagingConsumer(Properties props) {
        this.props = props;

        String[] symbols = ((String) props.get(StockSymbolsConfig)).split(",");
        for (var symbol : symbols) {
            currentSymbolValueMap.put(symbol, Double.NaN);
        }
    }

    /**
     * Initializes the messaging producer with the necessary configurations and resources
     * required for message production. Implementation may include setting up connections,
     * channels, or other producer-specific settings based on the chosen messaging provider.
     * <p>
     * This method must be implemented by subclasses (e.g., RabbitMqMessagingConsumer,
     * KafkaMessagingConsumer) to perform provider-specific initialization logic. It is
     * typically invoked before running the producer to ensure all configurations are
     * properly loaded.
     *
     * @throws RuntimeException if initialization fails due to invalid configurations or
     *         issues with the underlying messaging system.
     */

    public abstract void init() throws IOException, TimeoutException;
    /**
     * Executes the main operation of the messaging producer, responsible for continuously or
     * discretely sending messages to the configured messaging system (e.g., Kafka or RabbitMQ)
     * based on the producer's implementation. The method retrieves message data, processes
     * it if required, and sends messages to the appropriate channel.
     * <p>
     * Subclasses must provide their specific implementation for message production, including
     * defining how to connect, prepare, and send messages to the target system.
     * <p>
     * This method can operate in two modes:
     * - Continuous mode: Messages are sent in a loop until the producer's "continuousMode" flag is
     *   set to false.
     * - Non-continuous mode: Only one message is sent.
     * <p>
     * Implementations are responsible for respecting the properties configured in the producer
     * (e.g., send intervals, channel configurations) and handling connection or transmission
     * errors appropriately.
     *
     * @throws IOException if there is an I/O error during message production.
     * @throws InterruptedException if the thread executing this method is interrupted.
     * @throws TimeoutException if a timeout occurs while attempting to send a message.
     */
    public abstract void run() throws IOException, InterruptedException, TimeoutException;


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
