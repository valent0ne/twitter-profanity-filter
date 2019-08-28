package it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfiguration {
    private static Logger LOGGER = LoggerFactory
            .getLogger(KafkaConfiguration.class);

    private static Properties properties;

    public static void init() {
        try (InputStream input = KafkaConfiguration.class
                .getClassLoader().getResourceAsStream("kafka.properties")) {

            properties = new Properties();

            if (input == null) {
                LOGGER.error("Unable to find kafka.properties");
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
