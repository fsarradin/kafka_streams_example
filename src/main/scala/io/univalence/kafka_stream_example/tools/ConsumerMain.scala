package io.univalence.kafka_stream_example.tools

import java.time.Duration
import java.util.UUID
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.streams.scala.Serdes

object ConsumerMain {
  import scala.jdk.CollectionConverters._

  def main(args: Array[String]): Unit = {
    val consumer = new KafkaConsumer[String, String](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.GROUP_ID_CONFIG -> s"projection-consumer-${UUID.randomUUID()}"
      ).asJava,
      Serdes.String.deserializer(),
      Serdes.String.deserializer()
    )

    consumer.subscribe(topics.asJava)

    while (true) {
      val records: List[ConsumerRecord[String, String]] =
        consumer.poll(Duration.ofSeconds(10))
          .asScala
          .toList

      for (record <- records) {
        categorizedTopics(record.topic()) match {
          case InputTopic => print(Console.RED    + "[INPUT>]  ")
          case OutputTopic => print(Console.GREEN + "[>OUTPUT] ")
          case DebugTopic => print(Console.YELLOW + "[#DEBUG]  ")
        }
        println(s"${record.topic()}#${record.partition()}@${record.offset()}:${record.timestamp()}${Console.RESET}")
        println(s"\t${Console.CYAN}${record.key()}${Console.RESET}")
        println(s"\t${record.value()}")
      }
    }
  }

}
