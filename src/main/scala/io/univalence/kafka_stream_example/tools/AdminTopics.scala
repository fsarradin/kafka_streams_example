package io.univalence.kafka_stream_example.tools

import io.univalence.kafka_stream_example.Main
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import scala.util.Using

object AdminTopics {
  import scala.jdk.CollectionConverters._

  val partitionCount = 4

  def main(args: Array[String]): Unit = {
    Using(
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
        ).asJava
      )
    ) { admin =>
//      deleteTopics(topics, admin)
      recreateTopics(topics, admin)
    }.get
  }

  def recreateTopics(topics: List[String], admin: AdminClient): Unit = {
//    deleteTopics(topics, admin)
    doCreate(topics, admin)
  }

  def deleteTopics(value: List[String], admin: AdminClient): Unit = {
    val currentTopics = admin.listTopics().names().get().asScala.toSet

    val existingTopics = currentTopics.intersect(topics.toSet)
    println(s"Deleting topics: ${existingTopics.mkString(", ")}")
    doDelete(existingTopics.toList, admin)

    val internalTopics =
      currentTopics.filter(topic =>
        topic.startsWith(Main.applicationName) && (
          topic.endsWith("-changelog") || topic.endsWith("-repartition")
        )
      )
    if (internalTopics.nonEmpty) {
      println(s"Deleting internal topics: ${internalTopics.mkString(", ")}")
      doDelete(internalTopics.toList, admin)
    }
  }

  def doCreate(topics: List[String], admin: AdminClient): Unit = {
    val nodes             = admin.describeCluster().nodes().get().asScala
    val nodeCount         = nodes.size
    val replicationFactor = Math.min(nodeCount, 3).toShort

    for (
      (topic, result) <- admin
        .createTopics(
          topics
            .map(topic =>
              new NewTopic(topic, partitionCount, replicationFactor)
            )
            .asJava
        )
        .values()
        .asScala
    ) {
      try {
        result.get(30, TimeUnit.SECONDS)
        println(
          s"Created topic: $topic with $partitionCount partition(s), $replicationFactor replica(s)"
        )
      } catch {
        case e: Exception =>
          println(s"ERROR: creating topic $topic")
          e.printStackTrace()
      }
    }
  }

  def doDelete(topics: List[String], admin: AdminClient): Unit =
    for (
      (topic, result) <- admin.deleteTopics(topics.asJava).values().asScala
    ) {
      try {
        result.get(30, TimeUnit.SECONDS)
      } catch {
        case e: Exception =>
          println(s"ERROR: deleting topic $topic")
          e.printStackTrace()
      }
    }

}
