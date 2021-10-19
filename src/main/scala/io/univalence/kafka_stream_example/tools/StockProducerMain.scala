package io.univalence.kafka_stream_example.tools

import io.univalence.kafka_stream_example.model.{Stock, StockKey}
import java.nio.charset.StandardCharsets
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.io.Source
import scala.util.Using

object StockProducerMain {
  import scala.jdk.CollectionConverters._

  val topic = "stock-stream"

  def main(args: Array[String]): Unit = {
    val properties =
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
      ).asJava

    Using(
      new KafkaProducer(
        properties,
        Stock.keySerde.serializer(),
        Stock.valueSerde.serializer()
      )
    ) { producer =>
      Using(Source.fromResource("data/stock.json")) { resource =>
        for (line <- resource.getLines()) {
          println(s"current line: $line")
          val value = Stock.valueSerde
            .deserializer()
            .deserialize("", line.getBytes(StandardCharsets.UTF_8))
          val key = StockKey(store = value.store, product = value.product)
          val record = new ProducerRecord[StockKey, Stock](topic, key, value)
          println(s"producing to $topic: $key => $value")
          producer.send(record)
        }
      }.get
    }.get
  }

}
