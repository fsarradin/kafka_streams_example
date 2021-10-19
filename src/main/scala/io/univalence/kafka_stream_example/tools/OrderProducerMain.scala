package io.univalence.kafka_stream_example.tools

import io.univalence.kafka_stream_example.model.{Order, OrderKey}
import java.nio.charset.StandardCharsets
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.io.Source
import scala.util.Using

object OrderProducerMain {
  import scala.jdk.CollectionConverters._

  val topic = "order-stream"

  def main(args: Array[String]): Unit = {
    val properties =
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
      ).asJava

    Using(
      new KafkaProducer(
        properties,
        Order.keySerde.serializer(),
        Order.valueSerde.serializer()
      )
    ) { producer =>
      Using(Source.fromResource("data/order.json")) { resource =>
        for (line <- resource.getLines()) {
          println(s"current line: $line")
          val value = Order.valueSerde
            .deserializer()
            .deserialize("", line.getBytes(StandardCharsets.UTF_8))
          val key    = OrderKey(orderId = value.orderId)
          val record = new ProducerRecord[OrderKey, Order](topic, key, value)
          println(s"producing to $topic: $key => $value")
          producer.send(record)
        }
      }.get
    }.get
  }

}
