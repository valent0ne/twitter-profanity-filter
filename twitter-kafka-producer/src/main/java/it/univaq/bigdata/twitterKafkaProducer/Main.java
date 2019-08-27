package it.univaq.bigdata.twitterKafkaProducer;

import it.univaq.bigdata.twitterKafkaProducer.config.KafkaConfiguration;
import it.univaq.bigdata.twitterKafkaProducer.config.TwitterConfiguration;
import it.univaq.bigdata.twitterKafkaProducer.producer.TwitterKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application's entry point
 */
public class Main {

    private static Logger LOGGER = LoggerFactory.getLogger(it.univaq.bigdata.twitterKafkaProducer.Main.class);


    public static void main(String[] args) {
        LOGGER.info("loading configuration...");
        TwitterConfiguration.init();
        KafkaConfiguration.init();

        LOGGER.info("running producer...");
        TwitterKafkaProducer producer = new TwitterKafkaProducer();
        producer.run();
    }
}
