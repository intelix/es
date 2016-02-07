package es.sink

import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import akka.actor._
import akka.pattern.Patterns
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.{Timeout, ByteString}
import com.typesafe.config.Config
import es.model.{BinaryPayload, Payload, StringPayload}
import es.sink.TCPConnectionManagerActor.Msg.TerminatedSuccessfully
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import rs.core.actors.StatefulActor
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.language.existentials
import scala.util.{Failure, Success}

object EventRoute {
  def apply(cfg: Config)(implicit sys: ActorSystem): Option[EventRoute] = {
    cfg.asOptString("type") flatMap {
      case "console" => Some(new ConsoleRoute(cfg))
      case "tcp" => Some(new TCPRoute(cfg))
      case _ => None
    }
  }
}


trait RouteHandle {
  def stop() = ???
}


trait EventProcessor {
  def accept(p: Payload): Boolean = true
}

trait Handle extends EventProcessor {
  def stop(): Future[Boolean]
}

trait WithFilters extends EventProcessor {
  protected val config: Config
  private lazy val filters: Set[String] = config.asString("filter.tags", "").split(",").map(_.trim).filterNot(_.isEmpty).toSet

  private def filtersMatch(p: Payload) = filters.isEmpty || p.tags.exists(filters.contains)

  override def accept(p: Payload): Boolean = if (super.accept(p)) filtersMatch(p) else false
}

trait HandleWithFilters extends Handle with WithFilters

case class ErrorKind(isFatal: Boolean, msg: String, cause: Option[Throwable])

trait EventRoute {
  def start(): Future[RouteHandle] = ???

  def start(cfg: Config, onError: ErrorKind => Unit)(implicit sys: ActorSystem): Handle
}


class ConsoleRoute extends EventRoute {
  override def start(cfg: Config, onError: (ErrorKind) => Unit)(implicit sys: ActorSystem): Handle =
    new HandleWithFilters {
      override def stop(): Future[Boolean] = Future.successful(true)

      override def accept(p: Payload): Boolean = p match {
        case p: StringPayload if super.accept(p) =>
          printf("@ %d: [%s] \"%s\"\n", p.ts, p.tags.mkString(","), p.data)
          true
        case p: BinaryPayload if super.accept(p) =>
          printf("@ %d: [%s] Binary length: %d\n", p.ts, p.tags.mkString(","), p.data.length)
          true
      }

      override protected val config: Config = cfg
    }

}


final private class ReactiveStreamsBasedHandle(override val config: Config, onError: (ErrorKind) => Unit, onStop: () => Future[Boolean]) extends HandleWithFilters with Publisher[Payload] {
  private val subscribers = new AtomicReference[Array[SubscriberSession]](Array())

  //TODO
  val idleStrategy = new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))


  case class SubscriberSession(subscription: Sub, subscriber: Subscriber[_ >: Payload]) {
    def accept(p: Payload): Boolean = if (subscription.isActive && subscription.consumeDemand()) {
      println(s"!>>> Produced next: $p")
      subscriber.onNext(p)
      true
    } else {
      println(s"!>>> Unable to produce, active: ${subscription.isActive}")
      !subscription.isActive
    }
  }


  override def stop(): Future[Boolean] = onStop()


  override def accept(p: Payload): Boolean =
    if (!super.accept(p)) false
    else {
      println(s"!>>> Received another event.... $p")
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

  class Sub extends Subscription {
    private final val active = new AtomicBoolean(true)
    private final val demand = new AtomicLong(0)

    override def cancel(): Unit = {
      println(s"!>>> Cancelled")
      active.set(false)
    }

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


  override def subscribe(subscriber: Subscriber[_ >: Payload]): Unit = {
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


}

class TCPRouteProper extends EventRoute {
  override def start(cfg: Config, onError: (ErrorKind) => Unit)(implicit sys: ActorSystem): Handle = {
    val ref = sys.actorOf(Props(classOf[TCPConnectionManagerActor], cfg, onError))
    def stop() = Patterns.ask(ref, TCPConnectionManagerActor.Msg.RequestStop, Timeout(10, SECONDS)).map {
      case TerminatedSuccessfully => true
      case _ => false
    }
    val handle = new ReactiveStreamsBasedHandle(cfg, onError, stop)
    ref ! handle
    handle
  }
}

object TCPConnectionManagerActor {
  object Msg {
    case object RequestStop
    case object TerminatedSuccessfully
  }
}

class TCPConnectionManagerActor(cfg: Config, onError: (ErrorKind) => Unit, publisher: Publisher[Payload]) extends StatefulActor {
  override val evtSource: EvtSource = "TCPConnectionManagerActor"

  val host = cfg.asOptString("host")


}


/*

val decider: Supervision.Decider = {
      case x =>
        onError(ErrorKind(isFatal = true, "Flow failed", Some(x)))
        Supervision.Stop
    }

    implicit val materializer =
      ActorMaterializer(ActorMaterializerSettings(sys).withSupervisionStrategy(decider).withDebugLogging(enable = true))

    val maybeConnection = for (
      host <- cfg.asOptString("host");
      port <- cfg.asOptInt("port")
    ) yield Tcp().outgoingConnection("127.0.0.1", 12345)

    maybeConnection
 */

class TCPRoute(cfg: Config, onError: ErrorKind => Unit)(implicit sys: ActorSystem) extends FlowRoute(cfg, onError) {


  override protected def init(onError: (ErrorKind) => Unit): OnStop = {

    for (
      host <- cfg.asOptString("host")
    )


    val decider: Supervision.Decider = {
      case x =>
        onError(ErrorKind(isFatal = true, "Flow failed", Some(x)))
        Supervision.Stop
    }
    implicit val materializer =
      ActorMaterializer(ActorMaterializerSettings(sys).withSupervisionStrategy(decider).withDebugLogging(enable = true))



    val connection = Tcp().outgoingConnection("127.0.0.1", 12345)
    val pub = Source.fromPublisher(new Prod).map {
      case p: StringPayload => {
        println(s"!>>> Mapping next $p -> ${p.data.getBytes("UTF-8")}")
        ByteString.fromArray(p.data.getBytes("UTF-8"))
      }
      case p: BinaryPayload => ByteString.fromArray(p.data)
    }

    import akka.actor.ActorDSL._

    val ref = ActorDSL.actor(new Act {
      become {
        case x => println(s"!>>> Received: $x")
      }
      whenStopping {
        println(s"!>>> Stopped!")
      }
    })

    //  val sink = Sink.ignore
    val sink = Sink.actorRef(ref, PoisonPill)
    //  val sink = Sink.foreach[ByteString](s => println("!>>> Received: " + s.utf8String))
    val xx = Flow.fromSinkAndSource(sink, pub)

    implicit val ec = sys.dispatcher

    val framing = Framing.simpleFramingProtocol(Int.MaxValue).reversed
    val logger: BidiFlow[ByteString, ByteString, ByteString, ByteString, Unit] = {
      val top = Flow[ByteString].map {
        case x =>
          println("Top: " + x)
          x
      }
      val bottom = Flow[ByteString].map {
        case x =>
          println("Bottom: " + x)
          x
      }
      BidiFlow.fromFlows(top, bottom)
    }


    connection.join(logger.atop(framing).join(xx)).run() onComplete {
      case Success(c) => println(s"!>>>> Successfully connected, $c")
      case Failure(f) => println(s"!>>>> Connection failed $f")
    }

  }
}


abstract class ProperFlowRoute(cfg: Config, onError: ErrorKind => Unit) extends EventRoute(cfg, onError) {
  override protected def init(onError: (ErrorKind) => Unit): OnStop = {

  }
}


abstract class FlowRoute(cfg: Config, onError: ErrorKind => Unit) extends EventRoute(cfg, onError) {

  //TODO
  val idleStrategy = new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))


  case class SubscriberSession(subscription: Sub, subscriber: Subscriber[_ >: Payload]) {
    def accept(p: Payload): Boolean = if (subscription.isActive && subscription.consumeDemand()) {
      println(s"!>>> Produced next: $p")
      subscriber.onNext(p)
      true
    } else {
      println(s"!>>> Unable to produce, active: ${subscription.isActive}")
      !subscription.isActive
    }
  }

  class Sub extends Subscription {
    private final val active = new AtomicBoolean(true)
    private final val demand = new AtomicLong(0)

    override def cancel(): Unit = {
      println(s"!>>> Cancelled")
      active.set(false)
    }

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
      println(s"!>>> Received another event.... $p")
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
