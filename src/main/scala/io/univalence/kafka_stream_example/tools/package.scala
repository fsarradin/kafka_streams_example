package io.univalence.kafka_stream_example

package object tools {
  val inputTopics: List[String] =
    List(
      "stock-stream",
      "order-stream"
    )
  val debugTopics =
    List(
      "projection-on-stock",
      "projection-on-order",
      "order-table-output"
    )
  private val outputTopics =
    List(
      "projection-stream"
    )
  val topics: List[String] =
    inputTopics ++ outputTopics ++ debugTopics

  val categorizedTopics: Map[String, TopicKind] =
    (
      inputTopics.map(_       -> InputTopic)
        ++ outputTopics.map(_ -> OutputTopic)
        ++ debugTopics.map(_ -> DebugTopic)
    ).toMap

  sealed trait TopicKind
  final case object InputTopic  extends TopicKind
  final case object OutputTopic extends TopicKind
  final case object DebugTopic  extends TopicKind

}
