package io.univalence.kafka_stream_example

import cats.effect.{ExitCode, IO, IOApp}
import com.carrefour.phenix.atp.kafka_stream_example.model._
import io.univalence.kafka_stream_example.model.{Order, OrderInfo, OrderItem, OrderKey, OrderStock, Projection, ProjectionLine, Stock, StockKey}
import io.univalence.kafka_stream_example.utils.WebService
import java.time.{Instant, LocalDate, ZoneId}
import java.util.{Properties, UUID}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends IOApp {
  import scala.jdk.CollectionConverters._

  val defaultHttpPort = 10180
  val dayCount        = 7
  val applicationName = "kafka-streams-example"

  /* This topics needs to be created in Kafka before running the
   * service.
   *
   * If you use a local Kafka cluster with a single broker, we
   * recommend to divide each topic in 3 partitions and use a single
   * replica. If you use many brokers, increase the number of replica.
   */
  // input topics
  val StockTopic = "stock-stream"
  val OrderTopic = "order-stream"
  // output topic
  val ProjectionTopic = "projection-stream"
  // debug topics
  val ProjectionOnStockTopic = "projection-on-stock"
  val ProjectionOnOrderTopic = "projection-on-order"
  val OrderTableTopic = "order-table-output"

  /** Create the topology for ATP simplified.
    *
    * @param builder
    *   Kafka Streams topology/stream builder
    * @param dayCount
    *   number of days for the projection
    * @param currentTime
    *   call-by-name parameter to get current time
    */
  def createTopology(
      builder: StreamsBuilder,
      dayCount: Int,
      currentTime: => Instant
  ): Unit = {
    /* Create the stock KStream from the stock topic.
     *
     * You should define the serdes for the key and the value.
     */
    val stocks: KStream[StockKey, Stock] =
      builder.stream(StockTopic)(
        Consumed.`with`(Stock.keySerde, Stock.valueSerde)
      )

    /* Create a KStream for orders the same way.
     */
    val order: KStream[OrderKey, Order] =
      builder.stream(OrderTopic)(
        Consumed.`with`(Order.keySerde, Order.valueSerde)
      )

    /* Use Kafka Streams operation to reorganize the orders according
     * to the store and the product.
     *
     * `flatMap` takes a function that creates a collection of values.
     * The collection is then cut item by item to send down messages.
     *
     * `->` notation creates a tuple between left-side and right-side
     */
    val orderByStock: KStream[StockKey, OrderStock] =
      order
        .flatMap { (_, order) =>
          order.items
            .map(item =>
              StockKey(
                store = order.store,
                product = item.product
              )
                -> createOrderStockFrom(order, item)
            )
        }

    /* Reorganized orders are then stored in a KTable.
     *
     * To do so, we have to indicate in the `reduce` operation how to
     * merge the incoming order and the one stored to update the
     * KTable.
     *
     * The data in KTable will be "materialized", meaning that a
     * RocksDB key-value store will be created. The name of the store
     * is optional, but it is a very good idea to give one.
     */
    val orderByStockTable: KTable[StockKey, OrderStock] =
      orderByStock
        .groupByKey(Grouped.`with`(OrderStock.keySerde, OrderStock.valueSerde))
        .reduce { (order1, order2) =>
          if (order1.info.exists(_.quantity <= 0.0) || order2.info.exists(_.quantity <= 0.0))
            null
          else
            mergeOrders(order1, Option(order2))
        }(
          Materialized
            .as("order-table")(OrderStock.keySerde, OrderStock.valueSerde)
        )

    orderByStockTable
      .toStream
      .to(OrderTableTopic)(Produced.`with`(Stock.keySerde, OrderStock.valueSerde))

    /* With a leftJoin, we connect stock data to order data.
     *
     * You have to give a function that merges stock entities and
     * order entities that matches according to the key. As it is a
     * leftJoin, it is possible that there is no matching order. In
     * this case a `null` is associated to the stock.
     *
     * A join operation might result in data exchange between the
     * service instances/replicas. Those data will be sent in Kafka
     * topic automatically created by Kafka Streams. The named of the
     * topic is based on the `Joined` parameter.
     */
    val projectionOnStock: KStream[StockKey, Projection] =
      stocks
        .leftJoin(orderByStockTable)((stock, order) =>
          project(Option(stock), Option(order), currentTime, dayCount)
        )(
          Joined.`with`("join-stock-order")(
            Stock.keySerde,
            Stock.valueSerde,
            OrderStock.valueSerde
          )
        )
        .flatMapValues(_.toList)

    projectionOnStock.to(ProjectionOnStockTopic)(
      Produced.`with`(Projection.keySerde, Projection.valueSerde)
    )

    /* --------------------
     * Symmetrical pipeline to ensure projection updates based on
     * updates from the order side.
     */

    val stockTable: KTable[StockKey, Stock] =
      stocks
        .groupByKey(Grouped.`with`(Stock.keySerde, Stock.valueSerde))
        .reduce((stock1, stock2) => moreRecentOf(stock1, stock2))(
          Materialized.as("stock-table")(Stock.keySerde, Stock.valueSerde)
        )

    val orderByStockStream: KStream[StockKey, OrderStock] =
      orderByStock.leftJoin(orderByStockTable)(
        (orderFromStream, orderFromTable) =>
          mergeOrders(orderFromStream, Option(orderFromTable))
      )(
        Joined.`with`("join-order-stream-table")(
          OrderStock.keySerde,
          OrderStock.valueSerde,
          OrderStock.valueSerde
        )
      )

    val projectionOnOrder: KStream[StockKey, Projection] =
      orderByStockStream
        .leftJoin(stockTable)((order, stock) =>
          project(Option(stock), Option(order), currentTime, dayCount)
        )(
          Joined.`with`("join-order-stock")(
            OrderStock.keySerde,
            OrderStock.valueSerde,
            Stock.valueSerde
          )
        )
        .flatMapValues(_.toList)

    projectionOnOrder.to(ProjectionOnOrderTopic)(
      Produced.`with`(Projection.keySerde, Projection.valueSerde)
    )

    // --------------------

    /* The two projection pipeline are merged in a view to send
     * projection data in a single topic.
     */
    val projections: KStream[StockKey, Projection] =
      projectionOnStock.merge(projectionOnOrder)

    projections.to(ProjectionTopic)(
      Produced.`with`(Projection.keySerde, Projection.valueSerde)
    )
  }

  def createOrderStockFrom(order: Order, item: OrderItem): OrderStock = {
    OrderStock(
      store = order.store,
      product = item.product,
      info = List(
        OrderInfo(
          quantity = item.quantity,
          orderId = order.orderId,
          createdAt = order.createdAt,
          deliverAt = order.deliverAt,
          status = order.status
        )
      )
    )
  }

  // * Service entrypoint *
  def run(args: List[String]): IO[ExitCode] = {
    val numThreads = java.lang.Runtime.getRuntime.availableProcessors().toString

    val (parameters, values, _) =
      args.foldLeft(
        (Map.empty[String, String], List.empty[String], Option.empty[String])
      ) {
        case ((parameters, values, None), value) =>
          if (value.startsWith("--"))
            (parameters, values, Option(value.substring(2)))
          else
            (parameters, values :+ value, None)
        case ((parameters, values, Some(current)), value) =>
          if (value.startsWith("--"))
            (parameters + (current -> ""), values, Option(value.substring(2)))
          else
            (parameters + (current -> value), values, None)
      }

    val httpPort: Int =
      parameters
        .get("port")
        .map(_.toInt)
        .getOrElse(defaultHttpPort)

    val groupId: String =
      parameters.getOrElse("group.id", UUID.randomUUID().toString)

    val ksProperties: Properties = new Properties()
    ksProperties.putAll(
      Map[String, AnyRef](
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
        /* The application.id will determine the group.id but also
         * the directory name where the KTable data will be stored.
         *
         * If you modify application.id, you will create a new
         * consumer group that will consume input topics from the very
         * beginning and you will create a new directory structure for
         * your data storages. Thus, it is like deploying a brand new
         * application.
         */
        StreamsConfig.APPLICATION_ID_CONFIG -> s"$applicationName-$groupId",
        /* Kafka Streams can parallelize the work if you have multiple
         * cores.
         *
         * By default the value is one. Fix the value to the maximum
         * number of cores available in scalable environment.
         */
        StreamsConfig.NUM_STREAM_THREADS_CONFIG -> numThreads,
        /* This parameter allows Kafka Streams to perform optimization
         * on your topology.
         *
         * The parameter is disable by default.
         */
        StreamsConfig.TOPOLOGY_OPTIMIZATION -> StreamsConfig.OPTIMIZE
      ).asJava
    )

    val topology: Topology = getTopology

    val kafkaStreams = new KafkaStreams(topology, ksProperties)
    sys.addShutdownHook { kafkaStreams.close() }
    kafkaStreams.start()

    val webService = new WebService(kafkaStreams, topology)
    val router = Router(
      "/store"    -> webService.storeRoutes,
      "/topology" -> webService.topologyRoutes
    ).orNotFound

    BlazeServerBuilder[IO](global)
      .bindHttp(httpPort, "localhost")
      .withHttpApp(router)
      .serve
      .compile
      .drain
      .as(ExitCode.Success)
  }

  def getTopology: Topology = {
    val builder = new StreamsBuilder()
    createTopology(builder, dayCount, Instant.now())

    builder.build()
  }

  def project(
      optionalStock: Option[Stock],
      optionalOrder: Option[OrderStock],
      createdAt: Instant,
      dayCount: Int
  ): Option[Projection] =
    (optionalStock, optionalOrder) match {
      case (Some(stock), Some(order)) =>
        Some(project(stock, order, createdAt, dayCount))

      case (Some(stock), _) =>
        val projectionLines: List[ProjectionLine] =
          (1 to dayCount).flatMap { day =>
            List(
              ProjectionLine(
                day = day,
                slice = Projection.SliceA,
                quantity = stock.quantity
              ),
              ProjectionLine(
                day = day,
                slice = Projection.SliceB,
                quantity = stock.quantity
              )
            )
          }.toList

        Some(
          Projection(
            store = stock.store,
            product = stock.product,
            createdAt = createdAt,
            currentQuantity = stock.quantity,
            lines = projectionLines
          )
        )

      case _ => None
    }

  def project(
      stock: Stock,
      order: OrderStock,
      createdAt: Instant,
      dayCount: Int
  ): Projection = {
    val stockDate = instantToDate(stock.checkedAt)
    val boughtQuantities: Map[Int, Double] =
      order.info
        .map { info =>
          val orderInfoDate = instantToDate(info.deliverAt)
          val offset        = stockDate.until(orderInfoDate).getDays
          offset -> info.quantity
        }
        .groupMapReduce(_._1)(_._2)(_ + _)
        .withDefaultValue(0.0)

    val cumulBoughtQuantities: List[Double] =
      (1 until dayCount)
        .scanLeft(boughtQuantities(0)) { case (total, dayOffset) =>
          total + boughtQuantities(dayOffset)
        }
        .toList

    val projectionLines: List[ProjectionLine] =
      cumulBoughtQuantities
        .map(boughtQuantity => stock.quantity - boughtQuantity)
        .zipWithIndex
        .flatMap { case (projectedStock, dayOffset) =>
          List(
            ProjectionLine(
              day = dayOffset + 1,
              slice = Projection.SliceA,
              projectedStock
            ),
            ProjectionLine(
              day = dayOffset + 1,
              slice = Projection.SliceB,
              projectedStock
            )
          )
        }

    Projection(
      store = stock.store,
      product = stock.product,
      currentQuantity = stock.quantity,
      lines = projectionLines,
      createdAt = createdAt
    )
  }

  def moreRecentOf(stock1: Stock, stock2: Stock): Stock =
    if (stock1.checkedAt.isAfter(stock2.checkedAt))
      stock1
    else
      stock2

  def instantToDate(instant: Instant): LocalDate =
    LocalDate.ofInstant(
      instant,
      ZoneId.systemDefault()
    )

  def mergeOrders(
      order1: OrderStock,
      order2: Option[OrderStock]
  ): OrderStock =
    order2
      .map(o2 => order1.copy(info = (order1.info ++ o2.info).distinct))
      .getOrElse(order1)

}
