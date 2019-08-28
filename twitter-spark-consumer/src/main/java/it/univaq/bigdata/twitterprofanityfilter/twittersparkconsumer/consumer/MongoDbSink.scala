package it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.consumer

import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import it.univaq.bigdata.twitterprofanityfilter.twittersparkconsumer.config.MongoDbConfiguration
import org.apache.spark.sql.{ForeachWriter, Row}
import org.bson.Document

import scala.collection.mutable
import scala.collection.JavaConverters._

class MongoDbSink() extends ForeachWriter[Row] {
  val writeConfig: WriteConfig = WriteConfig(Map("uri" -> MongoDbConfiguration.get("address")))
  var mongoConnector: MongoConnector = _
  var rows: mutable.ArrayBuffer[Row] = _

  override def open(partitionId: Long, version: Long): Boolean = {
    mongoConnector = MongoConnector(writeConfig.asOptions)
    rows = new mutable.ArrayBuffer[Row]()
    true
  }

  override def process(value: Row): Unit = {
    rows.append(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (rows.nonEmpty) {
      mongoConnector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
        collection.insertMany(rows.map(row => {
          new Document(row.getAs("id_str"), row.getAs("text"))
        }).asJava)
      })
    }
  }


}