package com.wordpress.technicado

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object MainWithoutCheckpointing {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MainWithoutCheckpointing").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(10))

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

    ssc.start()
    ssc.awaitTermination()
  }

}
