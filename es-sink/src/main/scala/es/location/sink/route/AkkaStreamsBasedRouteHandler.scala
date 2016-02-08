package es.location.sink.route

import akka.actor.{ActorDSL, PoisonPill}
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import com.typesafe.config.Config
import es.location.sink.route.AkkaStreamsBasedRouteHandler.{Endpoint, StateData}
import es.model.{Payload, BinaryPayload, StringPayload}
import rs.core.actors.{ActorState, StatefulActor}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


object AkkaStreamsBasedRouteHandler {

  trait Endpoint

  case class StateData(endpoints: Vector[Endpoint], bridge: Option[Bridge] = None)

  object States {

    case object Disconnected extends ActorState

    case object ConnectionPending extends ActorState

    case object Connected extends ActorState

  }

}

abstract class AkkaStreamsBasedRouteHandler[EP >: Endpoint,MV](routeId: String, routeCfg: Config) extends StatefulActor[StateData] {

  import AkkaStreamsBasedRouteHandler._

  implicit val sys = context.system
  implicit val ec = context.dispatcher

  def parseEndpoints(cfg: Config): Try[List[EP]]


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
      parseEndpoints(routeCfg) match {
        case Success(es) =>
          self ! IntMsg.Connect
          stay() using d.copy(endpoints = Vector() ++ es)
        case Failure(f) =>
          // TODO log event
          stop(akka.actor.FSM.Failure("Invalid config"))
      }
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
    case Event(IntMsg.FlowTerminated, data) =>
      context.parent ! HandlerApi.Pending
      transitionTo(States.Disconnected) using data.copy(bridge = None)
  }

  otherwise {
    case Event(IntMsg.FlowTerminated, data) =>
      transitionTo(States.Disconnected) using data.copy(bridge = None)
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
      val bridge = new Bridge

      val publisher = Source.fromPublisher(bridge)



      val pub = Source.fromPublisher(bridge).map {
        case p: StringPayload =>
          println(s"!>>> Mapping next $p -> ${p.data.getBytes("UTF-8")}")
          ByteString.fromArray(p.data.getBytes("UTF-8"))
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

      val xx2 = Flow.fromSinkAndSource(sink, publisher)



      implicit val ec = sys.dispatcher

      val fl = buildFlow(connectTo)

      fl.join(xx2).run() onComplete {
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

  def buildFlow(ep: EP): Flow[ Payload, _ >: Any, Future[MV]]

  private object IntMsg {

    case object Configure

    case object Connect

    case class SuccessfullyConnected(matV: MV, b: Bridge)

    case class ConnectionAttemptFailed()

    case object FlowTerminated


  }

}
