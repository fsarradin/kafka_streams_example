package io.univalence.kafka_stream_example.learn

import cats.effect.{ExitCode, IO, IOApp}
import io.univalence.kafka_stream_example.utils.WebService
import java.util.Properties
import org.apache.kafka.streams.processor.{Processor, ProcessorContext}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
import scala.concurrent.ExecutionContext.Implicits.global

object DualOperationMain extends IOApp {
  import scala.jdk.CollectionConverters._

  val Input1Topic = "input1"
  val Input2Topic = "input2"

  def process(input1: KStream[String, String], input2: KStream[String, String]): KStream[String, String] =
    ???

  def createTopology(builder: StreamsBuilder): Unit = {
    val input1: KStream[String, String] =
      builder.stream(Input1Topic)(Consumed.`with`(Serdes.String, Serdes.String))
    val input2: KStream[String, String] =
      builder.stream(Input2Topic)(Consumed.`with`(Serdes.String, Serdes.String))

    val output =
      process(input1, input2)

    output.process(() => new Processor[String, String] {
      var context: ProcessorContext = _

      override def init(context: ProcessorContext): Unit = {
        this.context = context
      }

      override def process(key: String, value: String): Unit = {
        println(s"${Console.YELLOW}${context.topic()}#${context.partition()}@${context.offset()}:${context.timestamp()}")
        println(s"\t${Console.GREEN}$key${Console.RESET}")
        println(s"\t$value")
      }

      override def close(): Unit = ()
    })
  }

  def run(args: List[String]): IO[ExitCode] = {
    val numThreads = java.lang.Runtime.getRuntime.availableProcessors().toString

    val ksProperties: Properties = new Properties()
    ksProperties.putAll(
      Map[String, AnyRef](
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        StreamsConfig.APPLICATION_ID_CONFIG -> s"${getClass.getCanonicalName}",
        StreamsConfig.NUM_STREAM_THREADS_CONFIG -> numThreads,
        StreamsConfig.TOPOLOGY_OPTIMIZATION -> StreamsConfig.NO_OPTIMIZATION
      ).asJava
    )
    val builder = new StreamsBuilder()
    createTopology(builder)
    val topology = builder.build()

    val kafkaStreams = new KafkaStreams(topology, ksProperties)
    sys.addShutdownHook { kafkaStreams.close() }
    kafkaStreams.start()

    val webService = new WebService(kafkaStreams, topology)
    val router = Router(
      "/topology" -> webService.topologyRoutes
    ).orNotFound

    BlazeServerBuilder[IO](global)
      .bindHttp(???, "localhost")
      .withHttpApp(router)
      .serve
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
