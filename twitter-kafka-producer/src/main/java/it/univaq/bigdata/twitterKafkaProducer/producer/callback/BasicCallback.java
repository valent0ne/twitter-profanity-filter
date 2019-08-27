package it.univaq.bigdata.twitterKafkaProducer.producer.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicCallback implements Callback {

    private static Logger LOGGER = LoggerFactory.getLogger(it.univaq.bigdata.twitterKafkaProducer.producer.callback.BasicCallback.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            // LOGGER.info("Message acknowledged by partition {} with timestamp {}", metadata.partition(), metadata.timestamp());
        } else {
            LOGGER.error(exception.getMessage());
        }
    }
}
