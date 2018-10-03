package com.wordpress.technicado.common

import com.wordpress.technicado.checkpointing.MainWithCheckpointing.writerProps
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import collection.JavaConverters._

class KafkaUtility extends Serializable {

  def getKafkaDStream[K, V](ssc: StreamingContext,
                              readParams: Map[String, Object],
                              input_topic: Array[String]) = {
    KafkaUtils.createDirectStream[K, V](ssc, PreferConsistent, Subscribe[K, V](input_topic, readParams))
  }

  def writeRDDToKafka[K, V](output_topic: String, rdd: RDD[(K, V)]) = {
    rdd.foreachPartition(iter => {
      val producer = new KafkaProducer[K, V](writerProps.asJava)
      iter.foreach(pair => {
        producer.send(new ProducerRecord[K, V](output_topic, pair._1, pair._2))
      })
    })
  }
}
