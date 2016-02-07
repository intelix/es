package es.location.sink

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import akka.actor.FSM.Failure
import akka.actor.{ActorDSL, ActorRef, PoisonPill, Terminated}
import akka.stream.io.Framing
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import com.typesafe.config.Config
import es.location.sink.TCPRouteHandlerActor.{Endpoint, StateData, States}
import es.location.sink.route.HandlerApi
import es.model.{BinaryPayload, Payload, StringPayload}
import es.sink.EventProcessor
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import rs.core.actors.{ActorState, StatefulActor}
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy

import scala.annotation.tailrec
import scala.concurrent.duration.{MICROSECONDS, _}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object TCPRouteHandlerActor {

  case class Endpoint(host: String, port: Int)

  case class StateData(endpoints: Vector[Endpoint], bridge: Option[Bridge] = None)

  object States {

    case object Disconnected extends ActorState

    case object ConnectionPending extends ActorState

    case object Connected extends ActorState

  }

}

class TCPRouteHandlerActor(routeId: String, routeCfg: Config) extends StatefulActor[StateData] {
  override val evtSource: EvtSource = "TCPRouteHandlerActor"
  implicit val sys = context.system
  implicit val ec = context.dispatcher
  val decider: Supervision.Decider = {
    case x =>
      x.printStackTrace()
      Supervision.Stop
  }
  implicit val materializer =
    ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider).withDebugLogging(enable = false))

  startWith(States.Disconnected, StateData(Vector()))

  self ! IntMsg.Configure

  when(States.Disconnected) {
    case Event(IntMsg.Configure, d) =>
      (for (host <- routeCfg.asOptString("host"); port <- routeCfg.asOptInt("port")) yield Endpoint(host, port)) match {
        case Some(ep) =>
          self ! IntMsg.Connect
          stay() using d.copy(endpoints = Vector(ep))
        case None => stop(akka.actor.FSM.Failure("Invalid config"))
      }
    case Event(Terminated(r), _) => stay()
    case Event(IntMsg.Connect, data) =>
      transitionTo(States.ConnectionPending) using data.copy(endpoints = data.endpoints.tail :+ data.endpoints.head)
  }

  when(States.ConnectionPending) {
    case Event(IntMsg.SuccessfullyConnected(c, b), data) =>
      context.parent ! HandlerApi.Started(b)
      transitionTo(States.Connected) using data.copy(bridge = Some(b))
    case Event(IntMsg.ConnectionAttemptFailed(), data) => transitionTo(States.Disconnected)
  }

  when(States.Connected) {
    case Event(m@DatasourceStreamRef(r), data) =>
      context.watch(r)
      ref ! DatasourceStreamRef(r)
      stay() using data.copy(datasourceLinkRef = Some(r))
  }

  otherwise {
    case Event(IntMsg.FlowTerminated, data) =>
      context.parent ! HandlerApi.Pending
      transitionTo(States.Disconnected) using(data.copy(bridge = None))
    case Event(Terminated(r), data) =>
      transitionTo(States.Disconnected) using data.copy(datasourceLinkRef = None)
    case Event(m@DatasourceStreamRef(r), data) =>
      stay() using data.copy(datasourceLinkRef = Some(context.watch(r)))
  }

  onTransition {
    case States.ConnectionPending -> States.Disconnected =>
      log.info("Unable to connect to the datasource")
      setTimer("reconnectDelay", IntMsg.Connect, 1 second, repeat = false)
    case States.Connected -> States.Disconnected =>
      log.info("Lost connection to the datasource")
      self ! IntMsg.Connect
    case _ -> States.ConnectionPending =>
      val connectTo = stateData.endpoints.head
      log.info(s"Connecting to ${connectTo.host}:${connectTo.port}")

      val bridge = new Bridge

      val connection = Tcp().outgoingConnection("127.0.0.1", 12345)
      val pub = Source.fromPublisher(bridge).map {
        case p: StringPayload => {
          println(s"!>>> Mapping next $p -> ${p.data.getBytes("UTF-8")}")
          ByteString.fromArray(p.data.getBytes("UTF-8"))
        }
        case p: BinaryPayload => ByteString.fromArray(p.data)
      }

      import akka.actor.ActorDSL._

      val parent = self

      val ref = ActorDSL.actor(new Act {
        become {
          case x => println(s"!>>> Received: $x")
        }
        whenStopping {
          println(s"!>>> Stopped!")
          parent ! IntMsg.FlowTerminated
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
        case Success(c) =>
          println(s"!>>>> Successfully connected, $c")
          self ! IntMsg.SuccessfullyConnected(c, bridge)
        case scala.util.Failure(f) =>
          println(s"!>>>> Connection failed $f")
          self ! IntMsg.ConnectionAttemptFailed()
      }

    case _ -> States.Connected =>
      log.info("Successfully connected")
  }

  object IntMsg {

    case object Configure

    case object Connect

    case class SuccessfullyConnected(connection: OutgoingConnection, b: Bridge)

    case class ConnectionAttemptFailed()

    case object FlowTerminated

    case object ExposeFlow


  }

}


class Bridge extends Publisher[Payload] with EventProcessor {
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