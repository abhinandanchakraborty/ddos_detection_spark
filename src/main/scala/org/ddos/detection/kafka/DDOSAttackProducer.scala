

package org.ddos.detection.kafka
import java.util.Properties
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import scala.io.Source
import org.apache.kafka.clients.producer._

object DDOSAttackProducer extends App {

  val brokers = "velomobile-01.srvs.cloudkafka.com:9094,velomobile-02.srvs.cloudkafka.com:9094,velomobile-03.srvs.cloudkafka.com:9094";
  val username = "olzns47g";
  val password = "HA3t1e33HI_V8vwWOnvhLkpG__hZggSh";
  val topic = username + "-default";

  val jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
  val jaasCfg = String.format(jaasTemplate, username, password);

  val props = new Properties();
  props.put("bootstrap.servers", brokers);
  props.put("group.id", username + "-consumer");
  props.put("enable.auto.commit", "true");
  props.put("auto.commit.interval.ms", "1000");
  props.put("auto.offset.reset", "earliest");
  props.put("session.timeout.ms", "30000");
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  props.put("security.protocol", "SASL_SSL");
  props.put("sasl.mechanism", "SCRAM-SHA-256");
  props.put("sasl.jaas.config", jaasCfg);

  val producer = new KafkaProducer[String, String](props)

  val bufferedSource = Source.fromFile("apache-access-log.txt")
  var count = 1;
  for (line <- bufferedSource.getLines) {
    count += 1;
    println(line.toUpperCase)
    val record = new ProducerRecord[String, String](topic, count.toString(), line)
    producer.send(record)
  }

  producer.close()

}