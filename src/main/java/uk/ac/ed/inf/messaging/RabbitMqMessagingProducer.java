package uk.ac.ed.inf.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

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

    @Override
    public void init() {
        exchangeName = props.getProperty(RabbitMqExchange);
        factory = new ConnectionFactory();
        factory.setHost(props.getProperty(RabbitMqHost));
        factory.setPort(Integer.parseInt(props.getProperty(RabbitMqPort, "5672")));
    }

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
