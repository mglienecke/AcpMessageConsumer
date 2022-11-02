package uk.ac.ed.inf.kafkasamples;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

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

    private void process(String configFileName) throws IOException, InterruptedException {
        Properties kafkaPros = StockSymbolProducer.loadConfig(configFileName);

        var producer = new KafkaProducer<String, String>(kafkaPros);

        String[] symbols = ((String) kafkaPros.get(StockSymbolsConfig)).split(",");
        Double[] currentStockValue = new Double[symbols.length];

//
//        for (int i = 0; i < symbols.length; i++){
//
//            String json = readUrl("https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2021-07-22/2021-07-22?adjusted=true&sort=asc&limit=120&apiKey=4wIZm3ROOwpH8V8vfwmC3daRN2UNfKY4");
//
//            Gson gson = new Gson();
//            Page page = gson.fromJson(json, Page.class);
//
//            System.out.println(page.title);
//            for (Item item : page.items)
//                System.out.println("    " + item.title);
//        }

        for (int i = 0; i < symbols.length; i++) {
            currentStockValue[i] = new Double(i + 30);
        }

        int recordCount = 0;
        final String topic = kafkaPros.getProperty(KafkaTopicConfig);

        while (true) {
            for (int i = 0; i < symbols.length; i++) {
                if ((recordCount % 2) != 0){
                    currentStockValue[i] += new Random().nextDouble();
                } else {
                    currentStockValue[i] -= new Random().nextDouble();
                }

                final String stockSymbol = symbols[i];
                final String stockValue = currentStockValue[i].toString();

                producer.send(new ProducerRecord<>(topic, stockSymbol, stockValue), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, stockSymbol, stockValue);
                });

                recordCount++;
            }

            Thread.sleep(500);
            System.out.println(recordCount + " records sent to Kafka");
        }


    }

//
//    private static String readUrl(String urlString) throws Exception {
//        BufferedReader reader = null;
//        try {
//            URL url = new URL(urlString);
//            reader = new BufferedReader(new InputStreamReader(url.openStream()));
//            StringBuffer buffer = new StringBuffer();
//            int read;
//            char[] chars = new char[1024];
//            while ((read = reader.read(chars)) != -1)
//                buffer.append(chars, 0, read);
//
//            return buffer.toString();
//        } finally {
//            if (reader != null)
//                reader.close();
//        }
//    }



}

