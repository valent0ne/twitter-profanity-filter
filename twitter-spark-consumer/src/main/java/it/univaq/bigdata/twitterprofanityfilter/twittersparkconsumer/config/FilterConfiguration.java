package it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class FilterConfiguration {

    public static List<String> bannedWords;
    private static Logger LOGGER = LoggerFactory
            .getLogger(FilterConfiguration.class);


    public static void init() {
        try (InputStream input = SparkConfiguration.class
                .getClassLoader().getResourceAsStream("banned-words.txt")) {

            if (input == null) {
                LOGGER.error("Unable to find banned-words.txt");
                return;
            }

            bannedWords = new ArrayList<>();
            Scanner s = new Scanner(input);
            while(s.hasNextLine()){
                bannedWords.add(s.nextLine());
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
