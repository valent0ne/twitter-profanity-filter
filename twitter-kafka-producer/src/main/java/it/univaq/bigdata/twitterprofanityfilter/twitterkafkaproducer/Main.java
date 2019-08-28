package it.univaq.bigdata.twitterprofanityfilter.twitterkafkaproducer;

import it.univaq.bigdata.twitterprofanityfilter.twitterkafkaproducer.config.TwitterConfiguration;
import it.univaq.bigdata.twitterprofanityfilter.twitterkafkaproducer.config.KafkaConfiguration;
import it.univaq.bigdata.twitterprofanityfilter.twitterkafkaproducer.producer.TwitterKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application's entry point
 */
public class Main {

    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) {
        LOGGER.info("loading configuration...");
        TwitterConfiguration.init();
        KafkaConfiguration.init();

        LOGGER.info("running producer...");
        TwitterKafkaProducer producer = new TwitterKafkaProducer();
        producer.run();
    }
}
