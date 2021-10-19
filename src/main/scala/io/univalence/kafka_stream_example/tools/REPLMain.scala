package io.univalence.kafka_stream_example.tools

import fastparse.SingleLineWhitespace._
import fastparse._
import io.univalence.kafka_stream_example.model._
import java.nio.charset.StandardCharsets
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.scala.Serdes
import scala.io.StdIn

object REPLMain {

  import scala.jdk.CollectionConverters._

  def topicNameParser[_: P]: P[String] = P(
    CharsWhile(c => !c.isWhitespace).!
  )
  def sendParser[_: P]: P[Command] =
    P(
      IgnoreCase("TO") ~ (topicNameParser.!) ~ IgnoreCase("SEND") ~ (AnyChar
        .rep(1))
        .! ~ End
    )
      .map { case (topic, data) => Command("SEND", topic, data) }
  def topicsParser[_: P]: P[Command] =
    P(IgnoreCase("TOPICS")).map(_ => Command("TOPICS"))
  def helpParser[_: P]: P[Command] =
    P(IgnoreCase("HELP")).map(_ => Command("HELP"))
  def quitParser[_: P]: P[Command] =
    P(IgnoreCase("QUIT")).map(_ => Command("QUIT"))

  def lineParser[_: P]: P[Command] = P(
    sendParser | topicsParser | helpParser | quitParser
  )

  def main(args: Array[String]): Unit = {
    var done = false

    val producer = new KafkaProducer[String, String](
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
      ).asJava,
      Serdes.String.serializer(),
      Serdes.String.serializer()
    )

    try {
      println("Send data to input topics")
      println("type HELP if needed")
      while (!done) {
        val line   = StdIn.readLine(s"${Console.MAGENTA}> ${Console.YELLOW}")
        print(Console.RESET)
        val result = parse(line, lineParser(_))
        result match {
          case Parsed.Success(cmd, _) =>
            cmd.name match {
              case "SEND" =>
                val result = send(producer, cmd.params(0), cmd.params(1).trim)
                println(s"sent to ${result.topic()}#${result
                  .partition()}@${result.offset()}:${result.timestamp()}")
              case "TOPICS" =>
                inputTopics.foreach(println)
              case "HELP" =>
                println("""Commands:
                    |TO [topic_name] SEND [data]    send data to a topic
                    |TOPICS                         list input topics
                    |HELP                           print this help
                    |QUIT                           quit the repl
                    |""".stripMargin)
              case "QUIT" =>
                done = true
            }
          case Parsed.Failure(_, _, extra) =>
            if (extra.input.length > 0)
              println(result)
        }
      }
    } finally {
      producer.close()
    }
  }

  def send(
      producer: KafkaProducer[String, String],
      topic: String,
      data: String
  ): RecordMetadata = {
    val key =
      topic match {
        case "stock-stream" => createStockKey(data)
        case "order-stream" => createOrderKey(data)
      }

    val record = new ProducerRecord[String, String](topic, key, data)
    producer.send(record).get
  }

  def createStockKey(data: String): String = {
    val value = Stock.valueSerde
      .deserializer()
      .deserialize("", data.getBytes(StandardCharsets.UTF_8))

    val key = StockKey(store = value.store, product = value.product)

    new String(
      Stock.keySerde.serializer().serialize("", key),
      StandardCharsets.UTF_8
    )
  }

  def createOrderKey(data: String): String = {
    val value = Order.valueSerde
      .deserializer()
      .deserialize("", data.getBytes(StandardCharsets.UTF_8))

    val key = OrderKey(orderId = value.orderId)

    new String(
      Order.keySerde.serializer().serialize("", key),
      StandardCharsets.UTF_8
    )
  }

  case class Command(name: String, params: String*)
}
