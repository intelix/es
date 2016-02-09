package es.location.sink.route

import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.config.Config
import es.location.sink.route.AkkaStreamsBasedRouteHandler.StateData
import es.model.Payload
import rs.core.actors.{ActorState, StatefulActor}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}


object AkkaStreamsBasedRouteHandler {

  case class StateData(bridge: Option[Bridge] = None)

  object States {

    case object Disconnected extends ActorState

    case object ConnectionPending extends ActorState

    case object Connected extends ActorState

  }

}

abstract class AkkaStreamsBasedRouteHandler(routeId: String, routeCfg: Config) extends StatefulActor[StateData] {

  import AkkaStreamsBasedRouteHandler._

  implicit val sys = context.system
  implicit val ec = context.dispatcher

  val decider: Supervision.Decider = {
    case x =>
      x.printStackTrace()
      Supervision.Stop
  }
  implicit val materializer =
    ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider).withDebugLogging(enable = false))

  startWith(States.Disconnected, StateData())

  self ! IntMsg.Connect

  when(States.Disconnected) {
    case Event(IntMsg.Connect, data) =>
      transitionTo(States.ConnectionPending)
  }

  when(States.ConnectionPending) {
    case Event(IntMsg.SuccessfullyConnected(v, b), data) =>
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
      val bridge = new Bridge

      val publisher = Source.fromPublisher(bridge)

      implicit val ec = sys.dispatcher
      runFlow(publisher, _ => self ! IntMsg.FlowTerminated) onComplete {
        case Success(v) => self ! IntMsg.SuccessfullyConnected(v, bridge)
        case Failure(f) => self ! IntMsg.ConnectionAttemptFailed()
      }

    case _ -> States.Connected =>
      log.info("Successfully connected")
  }


  def runFlow(source: Source[Payload, Unit], onError: Option[Throwable] => Unit): Future[Any]

  private object IntMsg {

    case object Connect

    case class SuccessfullyConnected[T](v: T, b: Bridge)

    case class ConnectionAttemptFailed()

    case object FlowTerminated


  }

}
