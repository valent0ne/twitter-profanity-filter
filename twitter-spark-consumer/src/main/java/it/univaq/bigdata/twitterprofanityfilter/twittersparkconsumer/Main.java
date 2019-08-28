package it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer;

import it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config.FilterConfiguration;
import it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config.MongoDbConfiguration;
import it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config.KafkaConfiguration;
import it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config.SparkConfiguration;
import it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.consumer.TwitterSparkConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application's entry point
 */
public class Main {

    private static Logger LOGGER = LoggerFactory.getLogger(Main.class);

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
