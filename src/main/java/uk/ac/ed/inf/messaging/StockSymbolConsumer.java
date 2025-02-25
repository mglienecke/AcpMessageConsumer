package uk.ac.ed.inf.messaging;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class StockSymbolConsumer {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        if (args.length != 1) {
            System.out.println("Please provide the configuration file path as a command line argument");
            System.exit(1);
        }

        var consumer = MessagingConsumer.getMessagingConsumer(args[0]);
        consumer.init();
        consumer.run();
    }
}

