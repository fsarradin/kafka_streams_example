package io.univalence.kafka_stream_example

import com.carrefour.phenix.atp.kafka_stream_example.model.{OrderInfo, OrderStock, Projection, ProjectionLine, Stock}
import io.univalence.kafka_stream_example.model.OrderStock
import java.time.Instant
import org.scalatest.funsuite.AnyFunSuiteLike

class MainTest extends AnyFunSuiteLike {

  test("project 1 order") {
    val stock: Stock =
      Stock(
        store = "store",
        product = "product",
        checkedAt = Instant.parse("2021-01-01T00:00:00Z"),
        quantity = 100.0
      )

    val order: OrderStock =
      OrderStock(
        store = "store",
        product = "product",
        info = List(
          OrderInfo(
            quantity = 10.0,
            orderId = "order-1",
            createdAt = Instant.parse("2021-01-01T10:00:00Z"),
            deliverAt = Instant.parse("2021-01-02T10:00:00Z"),
            status = "CAPTURED"
          )
        )
      )

    val projectionCreatedAt = Instant.now
    val projection: Projection =
      Main.project(
        stock = stock,
        order = order,
        createdAt = projectionCreatedAt,
        dayCount = 2
      )

    val result =
      Projection(
        store = "store",
        product = "product",
        createdAt = projectionCreatedAt,
        currentQuantity = 100.0,
        lines = List(
          ProjectionLine(1, Projection.SliceA, 100.0),
          ProjectionLine(1, Projection.SliceB, 100.0),
          ProjectionLine(2, Projection.SliceA, 90.0),
          ProjectionLine(2, Projection.SliceB, 90.0)
        )
      )

    assert(projection === result)
  }

  test("project 2 orders") {
    val stock: Stock =
      Stock(
        store = "store",
        product = "product",
        checkedAt = Instant.parse("2021-01-01T00:00:00Z"),
        quantity = 100.0
      )

    val order: OrderStock =
      OrderStock(
        store = "store",
        product = "product",
        info = List(
          OrderInfo(
            quantity = 10.0,
            orderId = "order-1",
            createdAt = Instant.parse("2021-01-01T10:00:00Z"),
            deliverAt = Instant.parse("2021-01-02T10:00:00Z"),
            status = "CAPTURED"
          ),
          OrderInfo(
            quantity = 20.0,
            orderId = "order-1",
            createdAt = Instant.parse("2021-01-01T10:00:00Z"),
            deliverAt = Instant.parse("2021-01-03T10:00:00Z"),
            status = "CAPTURED"
          )
        )
      )

    val projectionCreatedAt = Instant.now
    val projection: Projection =
      Main.project(
        stock = stock,
        order = order,
        createdAt = projectionCreatedAt,
        dayCount = 3
      )

    val result =
      Projection(
        store = "store",
        product = "product",
        createdAt = projectionCreatedAt,
        currentQuantity = 100.0,
        lines = List(
          ProjectionLine(1, Projection.SliceA, 100.0),
          ProjectionLine(1, Projection.SliceB, 100.0),
          ProjectionLine(2, Projection.SliceA, 90.0),
          ProjectionLine(2, Projection.SliceB, 90.0),
          ProjectionLine(3, Projection.SliceA, 70.0),
          ProjectionLine(3, Projection.SliceB, 70.0)
        )
      )

    assert(projection === result)
  }

}
