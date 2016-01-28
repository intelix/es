package es.sink.client.test

import java.util.concurrent.TimeUnit._

import es.sink.client.BackpressureStrategy.{Drop, RetryThenDrop}
import es.sink.client.{SinkClient, SinkConnectionConfig, SinkSessionMeta}
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy

import scala.language.postfixOps

object BasicClient extends App {

  trait Publisher {
    def publish(level: String, v: String): Int
  }


  val pub = new SinkPublisher(1, "a")
  val pub2 = new SinkPublisher(1, "b")


  new Thread() {
    override def run(): Unit = {
      for (i <- 1 to 10000) {
        val started = System.nanoTime()
        val res = pub.publish("info", "From thread 1: " + i)
        val diff = System.nanoTime() - started
        println("From thread 1 = " + res + " nanos: " + diff)
        Thread.sleep(1000)
      }
    }
  }.start()

  new Thread() {
    override def run(): Unit = {
      for (i <- 1 to 10000) {
        pub2.publish("info", "From thread 2: " + i)
        Thread.sleep(777)
      }
    }
  }.start()


  class SinkPublisher(streamId: Int, id: String) extends Publisher {


    val conn = SinkClient()
      .connect(SinkConnectionConfig(
        //        channel = "udp://localhost:40123",
        channel = "aeron:ipc",
        streamId = 1,
//        backpressureStrategy = RetryThenDrop(100, new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))),
        backpressureStrategy = Drop,
        maxMessageLength = None,
        meta = SinkSessionMeta("a=1", "b=abc", "id=" + id)))

    override def publish(level: String, v: String): Int = conn.postWithTags(v, "level=" + level)
  }


}
