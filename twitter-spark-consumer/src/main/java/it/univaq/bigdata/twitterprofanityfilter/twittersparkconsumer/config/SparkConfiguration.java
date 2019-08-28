package it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SparkConfiguration {
    private static Logger LOGGER = LoggerFactory
            .getLogger(SparkConfiguration.class);

    private static Properties properties;

    public static String jsonSchema;
    /*
    public static final String jsonSchema = "{\n" +
            "  \"created_at\": \"Mon Aug 19 17:24:22 +0000 2019\",\n" +
            "  \"id\": 1163501976455303168,\n" +
            "  \"id_str\": \"1163501976455303168\",\n" +
            "  \"text\": \"RT @MiddleEastMnt: The money is being spent to purchase drones from the Israeli companies to be used to provide intelligence to Frontex htt…\",\n" +
            "  \"source\": \"<a href=\\\"https:\\/\\/mobile.twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web App<\\/a>\",\n" +
            "  \"truncated\": false,\n" +
            "  \"in_reply_to_status_id\": null,\n" +
            "  \"in_reply_to_status_id_str\": null,\n" +
            "  \"in_reply_to_user_id\": null,\n" +
            "  \"in_reply_to_user_id_str\": null,\n" +
            "  \"in_reply_to_screen_name\": null,\n" +
            "  \"user\": {\n" +
            "    \"id\": 438026876,\n" +
            "    \"id_str\": \"438026876\",\n" +
            "    \"name\": \"Patricia Abdó\",\n" +
            "    \"screen_name\": \"Pagl1313\",\n" +
            "    \"location\": \"Distrito Federal\",\n" +
            "    \"url\": \"https:\\/\\/endefensadepalestina.wordpress.com\",\n" +
            "    \"description\": \"Editora de libros para jóvenes y niños, aficionada a la cocina y a la política\",\n" +
            "    \"translator_type\": \"regular\",\n" +
            "    \"protected\": false,\n" +
            "    \"verified\": false,\n" +
            "    \"followers_count\": 520,\n" +
            "    \"friends_count\": 510,\n" +
            "    \"listed_count\": 86,\n" +
            "    \"favourites_count\": 27018,\n" +
            "    \"statuses_count\": 77261,\n" +
            "    \"created_at\": \"Fri Dec 16 03:17:30 +0000 2011\",\n" +
            "    \"utc_offset\": null,\n" +
            "    \"time_zone\": null,\n" +
            "    \"geo_enabled\": false,\n" +
            "    \"lang\": null,\n" +
            "    \"contributors_enabled\": false,\n" +
            "    \"is_translator\": false,\n" +
            "    \"profile_background_color\": \"C3F00E\",\n" +
            "    \"profile_background_image_url\": \"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\n" +
            "    \"profile_background_image_url_https\": \"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\n" +
            "    \"profile_background_tile\": false,\n" +
            "    \"profile_link_color\": \"4A913C\",\n" +
            "    \"profile_sidebar_border_color\": \"C0DEED\",\n" +
            "    \"profile_sidebar_fill_color\": \"DDEEF6\",\n" +
            "    \"profile_text_color\": \"333333\",\n" +
            "    \"profile_use_background_image\": false,\n" +
            "    \"profile_image_url\": \"http:\\/\\/pbs.twimg.com\\/profile_images\\/808727021471698945\\/8483Rs5o_normal.jpg\",\n" +
            "    \"profile_image_url_https\": \"https:\\/\\/pbs.twimg.com\\/profile_images\\/808727021471698945\\/8483Rs5o_normal.jpg\",\n" +
            "    \"profile_banner_url\": \"https:\\/\\/pbs.twimg.com\\/profile_banners\\/438026876\\/1443497079\",\n" +
            "    \"default_profile\": false,\n" +
            "    \"default_profile_image\": false,\n" +
            "    \"following\": null,\n" +
            "    \"follow_request_sent\": null,\n" +
            "    \"notifications\": null\n" +
            "  },\n" +
            "  \"geo\": null,\n" +
            "  \"coordinates\": null,\n" +
            "  \"place\": null,\n" +
            "  \"contributors\": null,\n" +
            "  \"retweeted_status\": {\n" +
            "    \"created_at\": \"Mon Aug 19 17:15:17 +0000 2019\",\n" +
            "    \"id\": 1163499690618445824,\n" +
            "    \"id_str\": \"1163499690618445824\",\n" +
            "    \"text\": \"The money is being spent to purchase drones from the Israeli companies to be used to provide intelligence to Frontex https:\\/\\/t.co\\/8kueQv6NCL\",\n" +
            "    \"source\": \"<a href=\\\"https:\\/\\/www.hootsuite.com\\\" rel=\\\"nofollow\\\">Hootsuite Inc.<\\/a>\",\n" +
            "    \"truncated\": false,\n" +
            "    \"in_reply_to_status_id\": null,\n" +
            "    \"in_reply_to_status_id_str\": null,\n" +
            "    \"in_reply_to_user_id\": null,\n" +
            "    \"in_reply_to_user_id_str\": null,\n" +
            "    \"in_reply_to_screen_name\": null,\n" +
            "    \"user\": {\n" +
            "      \"id\": 81136269,\n" +
            "      \"id_str\": \"81136269\",\n" +
            "      \"name\": \"Middle East Monitor\",\n" +
            "      \"screen_name\": \"MiddleEastMnt\",\n" +
            "      \"location\": \"London, UK\",\n" +
            "      \"url\": \"https:\\/\\/www.middleeastmonitor.com\",\n" +
            "      \"description\": \"Bringing a fair & accurate coverage of the Middle East to the West \\/\\/ Community Guidelines apply: https:\\/\\/www.middleeastmonitor.com\\/community\\/\",\n" +
            "      \"translator_type\": \"none\",\n" +
            "      \"protected\": false,\n" +
            "      \"verified\": true,\n" +
            "      \"followers_count\": 95752,\n" +
            "      \"friends_count\": 1358,\n" +
            "      \"listed_count\": 2429,\n" +
            "      \"favourites_count\": 431,\n" +
            "      \"statuses_count\": 119436,\n" +
            "      \"created_at\": \"Fri Oct 09 15:43:11 +0000 2009\",\n" +
            "      \"utc_offset\": null,\n" +
            "      \"time_zone\": null,\n" +
            "      \"geo_enabled\": true,\n" +
            "      \"lang\": null,\n" +
            "      \"contributors_enabled\": false,\n" +
            "      \"is_translator\": false,\n" +
            "      \"profile_background_color\": \"FFFFFF\",\n" +
            "      \"profile_background_image_url\": \"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme7\\/bg.gif\",\n" +
            "      \"profile_background_image_url_https\": \"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme7\\/bg.gif\",\n" +
            "      \"profile_background_tile\": true,\n" +
            "      \"profile_link_color\": \"CB0000\",\n" +
            "      \"profile_sidebar_border_color\": \"FFFFFF\",\n" +
            "      \"profile_sidebar_fill_color\": \"F3F3F3\",\n" +
            "      \"profile_text_color\": \"333333\",\n" +
            "      \"profile_use_background_image\": true,\n" +
            "      \"profile_image_url\": \"http:\\/\\/pbs.twimg.com\\/profile_images\\/543502394466631680\\/nujXyCLm_normal.png\",\n" +
            "      \"profile_image_url_https\": \"https:\\/\\/pbs.twimg.com\\/profile_images\\/543502394466631680\\/nujXyCLm_normal.png\",\n" +
            "      \"profile_banner_url\": \"https:\\/\\/pbs.twimg.com\\/profile_banners\\/81136269\\/1560455472\",\n" +
            "      \"default_profile\": false,\n" +
            "      \"default_profile_image\": false,\n" +
            "      \"following\": null,\n" +
            "      \"follow_request_sent\": null,\n" +
            "      \"notifications\": null\n" +
            "    },\n" +
            "    \"geo\": null,\n" +
            "    \"coordinates\": null,\n" +
            "    \"place\": null,\n" +
            "    \"contributors\": null,\n" +
            "    \"is_quote_status\": false,\n" +
            "    \"quote_count\": 0,\n" +
            "    \"reply_count\": 0,\n" +
            "    \"retweet_count\": 2,\n" +
            "    \"favorite_count\": 3,\n" +
            "    \"entities\": {\n" +
            "      \"hashtags\": [],\n" +
            "      \"urls\": [\n" +
            "        {\n" +
            "          \"url\": \"https:\\/\\/t.co\\/8kueQv6NCL\",\n" +
            "          \"expanded_url\": \"https:\\/\\/www.middleeastmonitor.com\\/20190819-eu-using-israel-drones-to-track-migrant-boats-in-the-med\\/\",\n" +
            "          \"display_url\": \"middleeastmonitor.com\\/20190819-eu-us…\",\n" +
            "          \"indices\": [\n" +
            "            117,\n" +
            "            140\n" +
            "          ]\n" +
            "        }\n" +
            "      ],\n" +
            "      \"user_mentions\": [],\n" +
            "      \"symbols\": []\n" +
            "    },\n" +
            "    \"favorited\": false,\n" +
            "    \"retweeted\": false,\n" +
            "    \"possibly_sensitive\": false,\n" +
            "    \"filter_level\": \"low\",\n" +
            "    \"lang\": \"en\"\n" +
            "  },\n" +
            "  \"is_quote_status\": false,\n" +
            "  \"quote_count\": 0,\n" +
            "  \"reply_count\": 0,\n" +
            "  \"retweet_count\": 0,\n" +
            "  \"favorite_count\": 0,\n" +
            "  \"entities\": {\n" +
            "    \"hashtags\": [],\n" +
            "    \"urls\": [],\n" +
            "    \"user_mentions\": [\n" +
            "      {\n" +
            "        \"screen_name\": \"MiddleEastMnt\",\n" +
            "        \"name\": \"Middle East Monitor\",\n" +
            "        \"id\": 81136269,\n" +
            "        \"id_str\": \"81136269\",\n" +
            "        \"indices\": [\n" +
            "          3,\n" +
            "          17\n" +
            "        ]\n" +
            "      }\n" +
            "    ],\n" +
            "    \"symbols\": []\n" +
            "  },\n" +
            "  \"favorited\": false,\n" +
            "  \"retweeted\": false,\n" +
            "  \"filter_level\": \"low\",\n" +
            "  \"lang\": \"en\",\n" +
            "  \"timestamp_ms\": \"1566235462665\"\n" +
            "}\n";

     */

    public static void init() {

        try {
            jsonSchema = IOUtils.toString(SparkConfiguration
                    .class
                    .getClassLoader()
                    .getResourceAsStream("tweet.json"), StandardCharsets.US_ASCII);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        try (InputStream input = SparkConfiguration.class
                .getClassLoader().getResourceAsStream("spark.properties")) {

            properties = new Properties();

            if (input == null) {
                LOGGER.error("Unable to find spark.properties");
                return;
            }

            //load a properties file from class path, inside static method
            properties.load(input);

            System.setProperty("hadoop.home.dir", properties.getProperty("hadoop-home-dir"));

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String get(String propertyName) {
        return properties.getProperty(propertyName);
    }

}
