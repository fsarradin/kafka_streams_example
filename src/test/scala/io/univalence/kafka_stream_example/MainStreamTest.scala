package io.univalence.kafka_stream_example

import io.univalence.kafka_stream_example.model._
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Properties, UUID}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.funsuite.AnyFunSuiteLike

class MainStreamTest extends AnyFunSuiteLike {
  import scala.jdk.CollectionConverters._

  test("should react to new orders") {
    val store    = "store"
    val product  = "product"
    val orderId  = "order-1"
    val baseTime = Instant.parse("2021-01-01T00:00:00Z")

    val properties = new Properties()
    properties.putAll(
      Map[String, AnyRef](
        StreamsConfig.APPLICATION_ID_CONFIG -> s"${getClass.getCanonicalName}-${UUID.randomUUID()}",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "",
//        StreamsConfig.STATE_DIR_CONFIG -> Files.createTempDirectory("kafka-streams"),
      ).asJava
    )

    val builder = new StreamsBuilder()
    Main.createTopology(builder, 3, baseTime)

    val testDriver = new TopologyTestDriver(builder.build(), properties)
    val stockTopic =
      testDriver.createInputTopic(
        Main.StockTopic,
        Stock.keySerde.serializer(),
        Stock.valueSerde.serializer()
      )
    val orderTopic =
      testDriver.createInputTopic(
        Main.OrderTopic,
        Order.keySerde.serializer(),
        Order.valueSerde.serializer()
      )
    val projectionTopic =
      testDriver.createOutputTopic(
        Main.ProjectionTopic,
        Projection.keySerde.deserializer(),
        Projection.valueSerde.deserializer()
      )

    val stockKey = StockKey(store = store, product = product)
    val stock = Stock(
      store = store,
      product = product,
      checkedAt = baseTime,
      quantity = 100.0
    )
    stockTopic.pipeInput(stockKey, stock)

    val orderKey = OrderKey(orderId = orderId)
    val order = Order(
      orderId = orderId,
      store = store,
      createdAt = baseTime,
      items = List(
        OrderItem(product = product, quantity = 10.0)
      ),
      deliverAt = baseTime.plus(2, ChronoUnit.DAYS),
      status = "CAPTURED"
    )
    orderTopic.pipeInput(orderKey, order)

    val projections = projectionTopic.readKeyValuesToList().asScala

    val projection1 =
      Projection(
        store,
        product,
        baseTime,
        100.0,
        List(
          ProjectionLine(1, Projection.SliceA, 100.0),
          ProjectionLine(1, Projection.SliceB, 100.0),
          ProjectionLine(2, Projection.SliceA, 100.0),
          ProjectionLine(2, Projection.SliceB, 100.0),
          ProjectionLine(3, Projection.SliceA, 100.0),
          ProjectionLine(3, Projection.SliceB, 100.0)
        )
      )
    val projection2 =
      Projection(
        store,
        product,
        baseTime,
        100.0,
        List(
          ProjectionLine(1, Projection.SliceA, 100.0),
          ProjectionLine(1, Projection.SliceB, 100.0),
          ProjectionLine(2, Projection.SliceA, 100.0),
          ProjectionLine(2, Projection.SliceB, 100.0),
          ProjectionLine(3, Projection.SliceA, 90.0),
          ProjectionLine(3, Projection.SliceB, 90.0)
        )
      )

    assert(projections.size === 2)
    assert(projections(0).value === projection1)
    assert(projections(1).value === projection2)
  }

  test("should react to new stocks") {
    val store    = "store"
    val product  = "product"
    val orderId  = "order-1"
    val baseTime = Instant.parse("2021-01-01T00:00:00Z")

    val properties = new Properties()
    properties.putAll(
      Map[String, AnyRef](
        StreamsConfig.APPLICATION_ID_CONFIG -> s"${getClass.getCanonicalName}-${UUID.randomUUID()}",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "",
//        StreamsConfig.STATE_DIR_CONFIG -> Files.createTempDirectory("kafka-streams"),
      ).asJava
    )

    val builder = new StreamsBuilder()
    Main.createTopology(builder, 3, baseTime)

    val testDriver = new TopologyTestDriver(builder.build(), properties)
    val stockTopic =
      testDriver.createInputTopic(
        Main.StockTopic,
        Stock.keySerde.serializer(),
        Stock.valueSerde.serializer()
      )
    val orderTopic =
      testDriver.createInputTopic(
        Main.OrderTopic,
        Order.keySerde.serializer(),
        Order.valueSerde.serializer()
      )
    val projectionTopic =
      testDriver.createOutputTopic(
        Main.ProjectionTopic,
        Projection.keySerde.deserializer(),
        Projection.valueSerde.deserializer()
      )

    val orderKey = OrderKey(orderId = orderId)
    val order = Order(
      orderId = orderId,
      store = store,
      createdAt = baseTime,
      items = List(
        OrderItem(product = product, quantity = 10.0)
      ),
      deliverAt = baseTime.plus(2, ChronoUnit.DAYS),
      status = "CAPTURED"
    )
    orderTopic.pipeInput(orderKey, order)

    val stockKey = StockKey(store = store, product = product)
    val stock = Stock(
      store = store,
      product = product,
      checkedAt = baseTime,
      quantity = 100.0
    )
    stockTopic.pipeInput(stockKey, stock)

    val projections = projectionTopic.readKeyValuesToList().asScala

    val projection =
      Projection(
        store,
        product,
        baseTime,
        100.0,
        List(
          ProjectionLine(1, Projection.SliceA, 100.0),
          ProjectionLine(1, Projection.SliceB, 100.0),
          ProjectionLine(2, Projection.SliceA, 100.0),
          ProjectionLine(2, Projection.SliceB, 100.0),
          ProjectionLine(3, Projection.SliceA, 90.0),
          ProjectionLine(3, Projection.SliceB, 90.0)
        )
      )

    assert(projections.size === 1)
    assert(projections(0).value === projection)
  }

}
