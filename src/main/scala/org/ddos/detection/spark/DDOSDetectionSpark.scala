package org.ddos.detection.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.ddos.detection.util._

object DDOSDetectionSpark extends App {
  
  System.setProperty("hadoop.home.dir", "C:/Users/abhin/scalaworkspace/ddos_detection_spark/");

  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingDemo")
  val ssc = new StreamingContext(sparkConf, Seconds(60))
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()

  val brokers = "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
  val username = "olzns47g"
  val password = Encryption.decrypt("/5NaiTzJFsGv6xxF4aBPSDHviD2Utws+hJYN2MzJ3AfaDQZBwpNm9kWsSlVdFxJ5");
  val topics = "olzns47g-default"

  val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
  val jaasCfg = String.format(jaasTemplate, username, password);

  val topicsSet = topics.split(",").toSet

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "group.id" -> "olzns47g-consumer",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "auto.commit.interval.ms" -> "1000",
    "auto.offset.reset" -> "earliest",
    "session.timeout.ms" -> "30000",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "security.protocol" -> "SASL_SSL",
    "sasl.mechanism" -> "SCRAM-SHA-256",
    "sasl.jaas.config" -> jaasCfg)

  val messages = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

  import spark.implicits._
  messages.foreachRDD(rdd => {
    val df = rdd.map(f => f.value().split("- -")(0)).toDF()
    df.groupBy("value").agg(count("*").alias("cnt")).orderBy(desc("cnt")).show(10,false)

  })

  // messages.map(record => (record.key(), record.value()))

  ssc.start()
  ssc.awaitTermination()

}