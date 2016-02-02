package es.sink

import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicReference}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Sink, Source, Flow, Tcp}
import akka.util.ByteString
import com.typesafe.config.Config
import es.model.{BinaryPayload, Payload, StringPayload}
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import rs.core.config.ConfigOps.wrap
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy

import scala.annotation.tailrec
import scala.language.existentials

object EventRoute {
  def apply(cfg: Config)(implicit sys: ActorSystem): Option[EventRoute] = {
    cfg.asOptString("type") flatMap {
      case "console" => Some(new ConsoleRoute(cfg))
      case "tcp" => Some(new TCPRoute(cfg))
      case _ => None
    }
  }
}

trait EventRoute {
  val cfg: Config
  private lazy val filters: Set[String] = cfg.asString("filter.tags", "").split(",").map(_.trim).filterNot(_.isEmpty).toSet

  private var chain: Array[PartialFunction[Payload, Boolean]] = Array()

  final def onEvent(f: PartialFunction[Payload, Boolean]) = chain = chain :+ f

  onEvent {
    case p => filtersMatch(p)
  }

  final def accept(p: Payload): Boolean = {
    var i = 0
    var accepted = true
    while (accepted && i < chain.length) {
      val next = chain(i)
      if (next.isDefinedAt(p)) accepted = next(p)
      i += 1
    }
    accepted
  }

  private def filtersMatch(p: Payload) = filters.isEmpty || p.tags.exists(filters.contains)
}

class ConsoleRoute(override val cfg: Config) extends EventRoute {
  onEvent {
    case p: StringPayload =>
      printf("@ %d: [%s] \"%s\"\n", p.ts, p.tags.mkString(","), p.data)
      true
    case p: BinaryPayload =>
      printf("@ %d: [%s] Binary length: %d\n", p.ts, p.tags.mkString(","), p.data.length)
      true
  }
}


class TCPRoute(override val cfg: Config)(implicit sys: ActorSystem) extends FlowRoute(cfg) {

  implicit val materializer = ActorMaterializer()

  val connection = Tcp().outgoingConnection("localhost", 12345)
  val pub = Source.fromPublisher(new Prod).map {
    case p: StringPayload => ByteString.fromArray(p.data.getBytes("UTF-8"))
    case p: BinaryPayload => ByteString.fromArray(p.data)
  }
//  val sink = Sink.ignore
  val sink = Sink.foreach[ByteString](s => println("!>>> Received: " + s.utf8String))
  val xx = Flow.fromSinkAndSource(sink, pub)

  connection.join(Framing.simpleFramingProtocol(1024)).join(xx).run()

}

class FlowRoute(override val cfg: Config) extends EventRoute {

  //TODO
  val idleStrategy = new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))


  case class SubscriberSession(subscription: Sub, subscriber: Subscriber[_ >: Payload]) {
    def accept(p: Payload): Boolean = if (subscription.isActive && subscription.consumeDemand()) {
      subscriber.onNext(p)
      true
    } else !subscription.isActive
  }

  class Sub extends Subscription {
    private final val active = new AtomicBoolean(true)
    private final val demand = new AtomicLong(0)
    override def cancel(): Unit = active.set(false)
    override def request(l: Long): Unit = {
      println(s"!>>>> Requested demand: $l")

      demand.addAndGet(l)
    }
    def isActive = active.get()
    def consumeDemand(): Boolean = if (isActive && demand.get() > 0) {
      demand.decrementAndGet()
      true
    } else false
  }

  val subscribers = new AtomicReference[Array[SubscriberSession]](Array())

  class Prod extends Publisher[Payload] {

    println("!>>>> Created Publisher")

    def add(subscriber: Subscriber[_ >: Payload]) = {
      println("!>>>> Adding subscriber...")
      val sub = new Sub()
      val session = SubscriberSession(sub, subscriber)
      @tailrec def addToList(): Unit = {
        val current = subscribers.get()
        if (!subscribers.compareAndSet(current, current :+ session)) addToList()
      }
      addToList()
      subscriber.onSubscribe(sub)
    }

    override def subscribe(subscriber: Subscriber[_ >: Payload]): Unit = add(subscriber)

  }

  onEvent {
    case p =>
      var i = 0
      val current = subscribers.get()
      while (i < current.length) {
        val next = current(i)
        idleStrategy.reset()
        while (!next.accept(p)) idleStrategy.idle()
        i += 1
      }
      true
  }


}
