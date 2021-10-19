package io.univalence.kafka_stream_example.model

import com.sksamuel.avro4s.JsonFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import java.time.Instant
import org.apache.kafka.common.serialization.Serde

case class OrderKey(orderId: String)

case class OrderItem(
    product: String,
    quantity: Double
)
case class Order(
    orderId: String,
    store: String,
    createdAt: Instant,
    items: List[OrderItem],
    deliverAt: Instant,
    status: String
)

object Order {
  val StatusCaptured  = "CAPTURED"
  val StatusPrepared  = "PREPARED"
  val StatusDelivered = "DELIVERED"
  val StatusCanceled  = "CANCELED"

  val keySerde: Serde[OrderKey] = new GenericSerde[OrderKey](JsonFormat)
  val valueSerde: Serde[Order]  = new GenericSerde[Order](JsonFormat)
}

case class OrderInfo(
    quantity: Double,
    orderId: String,
    createdAt: Instant,
    deliverAt: Instant,
    status: String
)
case class OrderStock(
    store: String,
    product: String,
    info: List[OrderInfo]
)
object OrderStock {
  val keySerde   = new GenericSerde[StockKey](JsonFormat)
  val valueSerde = new GenericSerde[OrderStock](JsonFormat)
}
