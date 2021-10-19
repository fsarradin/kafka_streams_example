# Kafka Streams example
This projection contains an example of Kafka Streams service. It has
been developed in Scala.

## Run all the thing!
To download:
* [Apache Kafka](https://kafka.apache.org/).
* [SBT](https://www.scala-sbt.org/) build tool.

### Launch Kafka
Then, in a first terminal, go to the Kafka directory, and launch
ZooKeeper, that is used by Kafka
```shell
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties
```

In a second terminal, go to the Kafka directory, and launch Kafka
```shell
$ ./bin/kafka-server-start.sh config/server.properties
```

### Launch Kafka Streams service
In a third terminal, launch the Kafka Streams service
```shell
$ sbt run
```

Type the number related to
`com.carrefour.phenix.atp.kafka_stream_example.Main`.

The service expose a Web API on port 10180. You can access to
http://127.0.0.1:10180/topology to see the topology of the service.
You can copy the content of the URL and paste it to
https://zz85.github.io/kafka-streams-viz/, which is a Kafka Streams
topology visualizer.

The stores available can be seen at
* stock-table: http://127.0.0.1:10180/store/stock-table
* order-table: http://127.0.0.1:10180/store/order-table

### Sending data

First, launch the generic consumer. In project directory
```shell
$ sbt run
```
And select related to
`com.carrefour.phenix.atp.kafka_stream_example.tools.ConsumerMain`.

In another terminal, in the project directory, launch the data sender
CLI
```shell
$ sbt run
```
And select option related to
`com.carrefour.phenix.atp.kafka_stream_example.learn.JLineCLIMain`.

On CLI prompt, you send data. The command looks like
`to <topic> send <data>`. `<topic>` is one of `stock-stream` or
`order-stream`. `<data>` is a string that will be sent as is. The key
is determined from data.
