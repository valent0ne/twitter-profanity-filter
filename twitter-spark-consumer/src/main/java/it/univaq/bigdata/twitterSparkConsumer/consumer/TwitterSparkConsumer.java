package it.univaq.bigdata.twitterSparkConsumer.consumer;

import it.univaq.bigdata.twitterSparkConsumer.config.FilterConfiguration;
import it.univaq.bigdata.twitterSparkConsumer.config.KafkaConfiguration;
import it.univaq.bigdata.twitterSparkConsumer.config.SparkConfiguration;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class TwitterSparkConsumer {

    private SparkSession sparkSession;
    private StructType schema;

    public TwitterSparkConsumer() {

        sparkSession = SparkSession
                .builder()
                .appName(SparkConfiguration.get("app-name"))
                .master(SparkConfiguration.get("master"))
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        // create a dummy dataset from a typical twitter json so to infer its structure
        schema = sparkSession
                .read()
                .json(sparkSession
                        .createDataset(Collections
                                .singletonList(SparkConfiguration.jsonSchema), Encoders.STRING()))
                .schema();

        //schema.printTreeString();

    }

    public void run() throws StreamingQueryException {

        Dataset<Row> inputDf = sparkSession
                // Read from kafka topic
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KafkaConfiguration.get("address"))
                .option("subscribe", KafkaConfiguration.get("topic"))
                .load();
        //inputDf.printSchema();

        // Interpret the value in the kafka topic as a string
        Dataset<Row> inputDfAsString = inputDf.selectExpr("CAST(value AS STRING)");

        // Convert it to json using the schema inferred in the init() function
        Dataset<Row> inputDfAsJson = inputDfAsString
                .select(from_json(inputDfAsString.col("value"), schema).as("tweet"));
        //inputDfAsJson.printSchema();

        // Flatten the dataset by removing the parent node previously created
        Dataset<Row> inputDfAsJsonFlattened = inputDfAsJson.select( "tweet.*");
        inputDfAsJsonFlattened.printSchema();

        Dataset<Row> filteredDf = inputDfAsJsonFlattened
                .filter(inputDfAsJsonFlattened.col("lang").equalTo("en")
                        .and(not(inputDfAsJsonFlattened.col("text")
                                .isin(FilterConfiguration.bannedWords.stream().toArray(String[]::new)))));


        // Output the result to the console
        /*
        StreamingQuery consoleOutput = filteredDf.writeStream()
                .outputMode("append")
                .format("console")
                .start();

         */

        long processingTime = Long.parseLong(SparkConfiguration.get("processing-time"));

        // Output the result to the database
        StreamingQuery databaseOutput = filteredDf.writeStream()
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(processingTime, TimeUnit.MILLISECONDS))
                .foreach(new MongoDbSink())
                .start();

        databaseOutput.awaitTermination();

    }


}
