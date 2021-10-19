package io.univalence.kafka_stream_example.model

import com.sksamuel.avro4s.JsonFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import java.time.Instant
import org.apache.kafka.common.serialization.Serde

case class StockKey(store: String, product: String)

case class Stock(
    store: String,
    product: String,
    checkedAt: Instant,
    quantity: Double
)

object Stock {
  val keySerde: Serde[StockKey] = new GenericSerde[StockKey](JsonFormat)
  val valueSerde: Serde[Stock]  = new GenericSerde[Stock](JsonFormat)
}
