package com.wordpress.technicado

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainWithCheckpointing {

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate("/tmp/spark_checkpoint/",
      () => createContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext: StreamingContext = {
    val conf: SparkConf = new SparkConf().setAppName("MainWithoutCheckpointing").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "0.0.0.0:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "some_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics: Array[String] = Array("test_source_topic")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val inputStream: DStream[(String, String)] = stream.map(record => (record.key, record.value))

    inputStream.print()

    ssc.checkpoint("/tmp/spark_checkpoint/")

    ssc
  }

}
