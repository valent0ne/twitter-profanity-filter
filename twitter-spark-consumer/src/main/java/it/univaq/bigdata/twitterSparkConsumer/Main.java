package it.univaq.bigdata.twitterSparkConsumer;

import it.univaq.bigdata.twitterSparkConsumer.config.MongoDbConfiguration;
import it.univaq.bigdata.twitterSparkConsumer.config.FilterConfiguration;
import it.univaq.bigdata.twitterSparkConsumer.config.KafkaConfiguration;
import it.univaq.bigdata.twitterSparkConsumer.config.SparkConfiguration;
import it.univaq.bigdata.twitterSparkConsumer.consumer.TwitterSparkConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application's entry point
 */
public class Main {

    private static Logger LOGGER = LoggerFactory.getLogger(it.univaq.bigdata.twitterSparkConsumer.Main.class);

    public static void main(String[] args) {

        LOGGER.info("loading configuration...");
        KafkaConfiguration.init();
        SparkConfiguration.init();
        MongoDbConfiguration.init();
        FilterConfiguration.init();

        /*
        for(String s : FilterConfiguration.bannedWords){
            LOGGER.info(s);
        }
         */

        LOGGER.info("running consumer...");
        TwitterSparkConsumer consumer = new TwitterSparkConsumer();

        try{
            consumer.run();
        }catch (Exception e){
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
