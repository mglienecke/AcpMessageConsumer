package uk.ac.ed.inf.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMqMessagingProducer is a concrete implementation of MessagingProducer that
 * facilitates sending messages to a RabbitMQ messaging system. It utilizes RabbitMQ
 * exchanges to publish the messages, using routing keys and corresponding payloads
 * to direct messages to appropriate queues.
 * <p>
 * This producer supports continuous and non-continuous message production modes,
 * as defined in the configuration properties. It requires connection configurations,
 * such as RabbitMQ host, port, and exchange details, to establish communication
 * with the RabbitMQ server.
 * <p>
 * The class is responsible for managing the RabbitMQ ConnectionFactory, creating
 * exchanges, building messages, and publishing them to the RabbitMQ system.
 * Additionally, it handles errors during message production, rethrowing them as
 * runtime exceptions when required.
 */
public class RabbitMqMessagingProducer extends MessagingProducer {
    public static final String RabbitMqOutboundTopicConfig = "rabbitmq.queues.outbound";
    public static final String RabbitMqInboundTopicConfig = "rabbitmq.queues.inbound";
    public static final String RabbitMqHost = "rabbitmq.host";
    public static final String RabbitMqPort = "rabbitmq.port";
    public static final String RabbitMqExchange = "rabbitmq.exchange";

    private ConnectionFactory factory = null;
    private String exchangeName = null;

    public RabbitMqMessagingProducer(Properties props) {
        super(props);
    }

    /**
     * Initializes the RabbitMqMessagingProducer with the necessary configurations for RabbitMQ.
     * <p>
     * This method retrieves the RabbitMQ exchange name, host, and port from the provided
     * configuration properties. It sets up a ConnectionFactory instance and configures it
     * with the host and port values. If the RabbitMqPort property is not explicitly defined
     * in the configuration, the default value of 5672 is used.
     * <p>
     * This method must be called prior to using the producer to ensure it is configured
     * correctly and ready for sending messages to the RabbitMQ system.
     *
     * @throws RuntimeException if any required configuration property is missing or invalid.
     */
    @Override
    public void init() {
        exchangeName = props.getProperty(RabbitMqExchange);
        factory = new ConnectionFactory();
        factory.setHost(props.getProperty(RabbitMqHost));
        factory.setPort(Integer.parseInt(props.getProperty(RabbitMqPort, "5672")));
    }

    /**
     * Executes the main operation of the RabbitMqMessagingProducer by continuously or
     * discretely publishing messages to a RabbitMQ exchange. Messages are published
     * in a direct exchange mode, using symbols as routing keys and associated values
     * as the payload.
     * <p>
     * The method works in either continuous mode or non-continuous mode based on the
     * configuration provided during producer setup:
     * - Continuous mode: Messages are sent repeatedly in a loop until the producer's
     *   "continuousMode" flag is set to false.
     * - Non-continuous mode: A single message is sent, and the loop exits immediately.
     * <p>
     * The producer connects to the RabbitMQ server, declares an exchange with a direct
     * type, and uses the "getNextSymbol" and "getNextValue" methods to retrieve the
     * routing key and message payload, respectively. Additionally, the method includes
     * a slight delay between iterations when operating in continuous mode.
     * <p>
     * Exceptions during the operation (e.g., connection issues or messaging system
     * errors) are caught and rethrown as a RuntimeException.
     *
     * @throws IOException if there is an I/O error during message production.
     * @throws TimeoutException if a timeout occurs during connection to RabbitMQ.
     */
    @Override
    public void run() throws IOException, TimeoutException {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(exchangeName, "direct");

            while (true) {
                final String symbol = getNextSymbol();
                final String value = getNextValue();

                channel.basicPublish(exchangeName, symbol, null, value.getBytes());
                System.out.println(" [x] Sent '" + symbol + "' : " + value);

                if (! continuousMode) {
                    break;
                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
