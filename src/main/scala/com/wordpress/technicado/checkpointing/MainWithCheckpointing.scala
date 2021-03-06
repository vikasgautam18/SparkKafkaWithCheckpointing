package com.wordpress.technicado.checkpointing

import java.lang

import com.wordpress.technicado.common.KafkaUtility
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainWithCheckpointing {

  val readParams: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "0.0.0.0:6667",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> "some_group",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: lang.Boolean)
  )

  val writerProps : Map[String, Object] = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "0.0.0.0:6667",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer])

  val output_topic = "test_target_topic"
  val input_topic: Array[String] = Array("test_source_topic")

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getOrCreate("/tmp/spark_checkpoint/",
      () => createContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext: StreamingContext = {
    val conf: SparkConf = new SparkConf().setAppName("MainWithoutCheckpointing").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    val kafkaUtility = new KafkaUtility


    val stream: InputDStream[ConsumerRecord[String, String]] = kafkaUtility.getKafkaDStream(ssc, readParams, input_topic)

    val inputStream: DStream[(String, String)] = stream.map(record => (record.key, record.value))
    inputStream.print()


    inputStream.foreachRDD(rdd => {
      kafkaUtility.writeRDDToKafka(output_topic, rdd)
    })

    ssc.checkpoint("/tmp/spark_checkpoint/")

    ssc
  }
}
