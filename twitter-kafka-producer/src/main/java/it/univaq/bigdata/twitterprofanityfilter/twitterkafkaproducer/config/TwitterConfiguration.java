package it.univaq.bigdata.twitterprofanityfilter.twitterkafkaproducer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TwitterConfiguration {

    private static Logger LOGGER = LoggerFactory
            .getLogger(TwitterConfiguration.class);

    private static Properties properties;

    public static void init() {
        try (InputStream input = TwitterConfiguration.class
                .getClassLoader().getResourceAsStream("twitter4j.properties")) {

            properties = new Properties();

            if (input == null) {
                LOGGER.error("Unable to find twitter4j.properties");
                return;
            }

            //load a properties file from class path, inside static method
            properties.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String get(String propertyName){
        return properties.getProperty(propertyName);
    }
}
