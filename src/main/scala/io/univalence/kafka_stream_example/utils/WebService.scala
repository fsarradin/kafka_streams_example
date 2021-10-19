package io.univalence.kafka_stream_example.utils

import cats.effect._
import io.univalence.kafka_stream_example.model.{OrderStock, Stock, StockKey}
import java.time.{Instant, ZoneId}
import java.util.concurrent.{ExecutorService, Executors}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, Topology}
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.headers._
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.ExecutionContext

class WebService(streams: KafkaStreams, topology: Topology) {
  import scala.jdk.CollectionConverters._

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val blockingPool: ExecutorService = Executors.newFixedThreadPool(4)
  val blocker: Blocker              = Blocker.liftExecutorService(blockingPool)
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val topologyRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      Ok(topology.describe().toString)
  }

  val storeRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root =>
      val out = s"""["stock-table", "order-table"]"""
      Ok(out).map(
        _.withContentType(`Content-Type`(MediaType.application.json, Charset.`UTF-8`))
      )

    case GET -> Root / "stock-table" =>
      val out: String =
        htmlOf[StockKey, Stock](
          "stock-table",
          List("store", "product", "checkedAt", "quantity"),
          stock => s"""<tr>
                |<td>${stock.store}</td>
                |<td>${stock.product}</td>
                |<td>${toReadable(stock.checkedAt)}</td>
                |<td class="number">${stock.quantity}</td>
                |</tr>""".stripMargin
        )

      Ok(out).map(
        _.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`))
      )

    case GET -> Root / "order-table" =>
      val out =
        htmlOf[StockKey, OrderStock](
          "order-table",
          List("store", "product", "info"),
          { order =>
            val info =
              order.info
                .map(oi =>
                  s"<li>${oi.orderId} => ${oi.quantity} (${oi.status} - ${toReadable(oi.createdAt)})</li>"
                )
                .mkString("\n")

            s"""<tr>
               |<td>${order.store}</td>
               |<td>${order.product}</td>
               |<td><ul>$info</ul></td>
               |</tr>""".stripMargin
          }
        )

      Ok(out).map(
        _.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`))
      )

    case GET -> Root / "join-order-stream-table" =>
      val out =
        htmlOf[StockKey, OrderStock](
          "join-order-stream-table",
          List("store", "product", "info"),
          { order =>
            val info =
              order.info
                .map(oi =>
                  s"<li>${oi.orderId} => ${oi.quantity} (${oi.status} - ${toReadable(oi.createdAt)})</li>"
                )
                .mkString("\n")

            s"""<tr>
               |<td>${order.store}</td>
               |<td>${order.product}</td>
               |<td><ul>$info</ul></td>
               |</tr>""".stripMargin
          }
        )

      Ok(out).map(
        _.withContentType(`Content-Type`(MediaType.text.html, Charset.`UTF-8`))
      )

    case request @ GET -> Root / "asset" / file =>
      StaticFile
        .fromResource(s"asset/$file", blocker, Some(request))
        .getOrElseF(NotFound())
  }

  def htmlOf[K, V](
      storeName: String,
      columns: List[String],
      itemFormatter: V => String
  ): String = {
    val store: ReadOnlyKeyValueStore[K, V] =
      streams.store(
        storeName,
        QueryableStoreTypes.keyValueStore[K, V]()
      )
    val data: Iterator[KeyValue[K, V]] = store.all().asScala
    val result =
      data
        .map(kv => itemFormatter(kv.value))
        .take(100)
        .mkString("\n")
    val header = columns.map(c => s"<th>$c</th>").mkString

    s"""<html>
       |<head>
       |<title>$storeName</title>
       |<link rel="stylesheet" href="asset/main.css">
       |</head>
       |<body>
       |<h1>$storeName</h1>
       |<table>
       |<tr>$header</tr>
       |$result
       |</table>
       |</body></html>""".stripMargin
  }

  def toReadable(instant: Instant): String =
    instant.atZone(ZoneId.systemDefault()).toLocalDateTime.toString

}
