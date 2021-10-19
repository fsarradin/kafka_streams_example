package io.univalence.kafka_stream_example.model

import com.sksamuel.avro4s.JsonFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import java.time.Instant

case class ProjectionLine(day: Int, slice: String, quantity: Double)
case class Projection(store: String, product: String, createdAt: Instant, currentQuantity: Double, lines: List[ProjectionLine])

object Projection {
  val SliceA = "A"
  val SliceB = "B"

  val DayCount = 7

  val keySerde = new GenericSerde[StockKey](JsonFormat)
  val valueSerde = new GenericSerde[Projection](JsonFormat)
}
