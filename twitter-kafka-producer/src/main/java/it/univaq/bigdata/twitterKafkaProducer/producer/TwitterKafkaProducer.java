package it.univaq.bigdata.twitterKafkaProducer.producer;

import it.univaq.bigdata.twitterKafkaProducer.producer.callback.BasicCallback;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import it.univaq.bigdata.twitterKafkaProducer.config.KafkaConfiguration;
import it.univaq.bigdata.twitterKafkaProducer.config.TwitterConfiguration;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class TwitterKafkaProducer {

    private Producer<Long, String> producer;
    private BasicCallback callback;

    private static Logger LOGGER = LoggerFactory.getLogger(it.univaq.bigdata.twitterKafkaProducer.producer.TwitterKafkaProducer.class);

    public TwitterKafkaProducer() {
        callback = new BasicCallback();
        producer = getProducer();
    }

    public void run() {
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                publish(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub
            }

            @Override
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub
            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(TwitterConfiguration.get("oauth.consumerKey"))
                .setOAuthConsumerSecret(TwitterConfiguration.get("oauth.consumerSecret"))
                .setOAuthAccessToken(TwitterConfiguration.get("oauth.accessToken"))
                .setOAuthAccessTokenSecret(TwitterConfiguration.get("oauth.accessTokenSecret"))
                .setJSONStoreEnabled(true);

        TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
        TwitterStream twitterStream = tf.getInstance();
        twitterStream.addListener(listener);

        //filter based on your choice of keywords
        String[] keywordsArray = TwitterConfiguration.get("keywords").split(":");
        // if no keywords are provided
        if (keywordsArray.length <= 1) {
            twitterStream.sample();
        } else {
            FilterQuery filter = new FilterQuery();
            filter.track(keywordsArray);
            twitterStream.filter(filter);
        }

    }


    // return an initialized kafka producer
    private Producer<Long, String> getProducer() {
        Properties properties = new Properties();

        // set the broker ip:port address
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.get("address"));
        /*
        acks=0   If set to zero then the producer will not wait for any acknowledgment from the server at all.
                 The record will be immediately added to the socket buffer and considered sent. No guarantee can be made
                 that the server has received the record in this case, and the retries configuration will not take
                 effect (as the client won't generally know of any failures). The offset given back for each record will
                 always be set to -1.
        acks=1   This will mean the leader will write the record to its local log but will respond without awaiting full
                 acknowledgement from all followers. In this case should the leader fail immediately after acknowledging
                 the record but before the followers have replicated it then the record will be lost.
        acks=all This means the leader will wait for the full set of in-sync replicas to acknowledge the record.
                 This guarantees that the record will not be lost as long as at least one in-sync replica remains alive.
                 This is the strongest available guarantee. This is equivalent to the acks=-1 setting.
         */
        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        /*
        The producer groups together any records that arrive in between request transmissions into a single batched
        request. Normally this occurs only under load when records arrive faster than they can be sent out. However in
        some circumstances the client may want to reduce the number of requests even under moderate load. This setting
        accomplishes this by adding a small amount of artificial delay—that is, rather than immediately sending out a
        record the producer will wait for up to the given delay to allow other records to be sent so that the sends can
        be batched together. This can be thought of as analogous to Nagle's algorithm in TCP. This setting gives the
        upper bound on the delay for batching: once we get batch.size worth of records for a partition it will be sent
        immediately regardless of this setting, however if we have fewer than this many bytes accumulated for this
        partition we will 'linger' for the specified time waiting for more records to show up. This setting defaults
        to 0 (i.e. no delay). Setting linger.ms=5, for example, would have the effect of reducing the number of requests
        sent but would add up to 5ms of latency to records sent in the absence of load.
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        // not necessary with acks=0
        //properties.put(ProducerConfig.RETRIES_CONFIG, 0);

        // an id string to pass to the server when making requests
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfiguration.get("client_id"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    // publish the passed tweet to a kafka topic
    private void publish(Status status) {
        String payload = TwitterObjectFactory.getRawJSON(status);
        //LOGGER.info("Fetched tweet with id {}", status.getId());
        long key = status.getId();
        // build a record
        ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfiguration.get("topic"), key, payload);
        // push the tweet to the kafka topic
        //LOGGER.info(payload);
        // synchronous publish
        //producer.send(record, callback).get();
        // asynchronous publish
        producer.send(record, callback);
        //LOGGER.info("Published tweet with id {}", status.getId());
    }
}
