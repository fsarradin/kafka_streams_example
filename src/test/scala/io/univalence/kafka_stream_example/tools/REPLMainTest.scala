package io.univalence.kafka_stream_example.tools

import com.carrefour.phenix.atp.kafka_stream_example.tools.REPLMain.Command
import fastparse._
import org.scalatest.funsuite.AnyFunSuiteLike

class REPLMainTest extends AnyFunSuiteLike {

  test("should parse topic name") {
    val result = parse("stream", REPLMain.topicNameParser(_))
    assert(result === Parsed.Success("stream", 6))
  }

  test("should parse topic name with dash") {
    val result = parse("some-stream", REPLMain.topicNameParser(_))
    assert(result === Parsed.Success("some-stream", 11))
  }

  test("should parse send command") {
    val cmd    = """TO a-stream SEND {"a": 1, "b": 2}"""
    val result = parse(cmd, REPLMain.sendParser(_))

    assert(
      result === Parsed.Success(
        Command("SEND", "a-stream", """{"a": 1, "b": 2}"""),
        33
      )
    )
  }

}
