package io.univalence.kafka_stream_example.learn

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import scala.util.Using

object AdminTopics {
  import scala.jdk.CollectionConverters._

  val topics = List("input", "input1", "input2")

  def main(args: Array[String]): Unit = {
    Using(
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
        ).asJava
      )
    ) { admin =>
//      deleteTopics(topics, admin)
      createTopics(topics, admin)
    }.get
  }

  def deleteTopics(topics: List[String], admin: AdminClient): Unit = {
    try {
      admin.deleteTopics(topics.asJava).all().get()
    } catch { case e: Throwable => e.printStackTrace() }
  }

  def createTopics(topics: List[String], admin: AdminClient): Unit = {
    admin
      .createTopics(
        topics.map(topic => new NewTopic(topic, 3, 1.toShort)).asJava
      )
      .all()
      .get()
  }
}
