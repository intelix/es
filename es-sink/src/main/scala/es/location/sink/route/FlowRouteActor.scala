package es.location.sink.route

import akka.actor.{ActorRef, Props, Terminated}
import com.typesafe.config.{Config, ConfigFactory}
import es.location.media.sub.MediaSubscriptionActor
import es.location.sink.SinkServiceActor.Started
import es.location.sink.route.FlowRouteActor.Data
import es.sink.EventProcessor
import rs.core.actors.{ActorState, StatefulActor}
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource

object FlowRouteActor {

  object Msg {

    case class Configure(cfg: String, enabled: Boolean)

    case class EventProcessorHandle(handle: EventProcessor)

    case class Plug(ref: ActorRef)

    case object Delete

    case object TerminateRoute

  }

  case class Data(
                   mediaSub: Option[ActorRef],
                   cfg: Option[String] = None,
                   routeHandlerRef: Option[ActorRef] = None,
                   eventProcessorHandle: Option[EventProcessor] = None,
                   lastError: Option[String] = None,
                   autoStartWith: Option[Msg.Configure] = None)

  object States {

    case object Unconfigured extends ActorState

    case object Starting extends ActorState

    case object Started extends ActorState

    case object Stopping extends ActorState

    case object Stopped extends ActorState

    case object Failed extends ActorState

  }

  object Evt {
    val SourceId = "FlowRoute"
  }

}

class FlowRouteActor(routeId: String) extends StatefulActor[Data] {

  import FlowRouteActor._

  override val evtSource: EvtSource = Evt.SourceId

  startWith(States.Unconfigured, Data(mediaSub = None))


  when(States.Starting) {
    case Event(m: Msg.Configure, d) =>
      d.routeHandlerRef.foreach(_ ! Msg.TerminateRoute)
      stay() using d.copy(autoStartWith = Some(m))
    case Event(Terminated(ref), d) if d.routeHandlerRef.contains(ref) && d.autoStartWith.isDefined => transitionTo(States.Stopped) using d.copy(routeHandlerRef = None)
    case Event(Terminated(ref), d) if d.routeHandlerRef.contains(ref) => transitionTo(States.Failed) using d.copy(routeHandlerRef = None)
    case Event(Msg.EventProcessorHandle(h), d) => transitionTo(States.Started) using d.copy(eventProcessorHandle = Some(h))
  }

  when(States.Started) {
    case Event(IntMsg.Stop(r), d) =>
      d.routeHandlerRef.foreach(_ ! Msg.TerminateRoute)
      transitionTo(States.Stopping)
  }

  when(States.Stopping) {
    case Event(m: Msg.Configure, d) => stay() using d.copy(autoStartWith = Some(m))
    case Event(IntMsg.Stopped(r), d) => transitionTo(States.Stopped) using d.copy(eventProcessorHandle = None, lastError = None)
    case Event(IntMsg.FailedToStop(r, f), d) => transitionTo(States.Failed) using d.copy(lastError = Some(f.map(_.getMessage).getOrElse("Unable to stop")))
  }

  otherwise {
    case Event(Msg.Plug(ref), d) =>
      d.eventProcessorHandle.foreach(ref ! MediaSubscriptionActor.Msg.AddRoute(_))
      stay() using d.copy(mediaSub = Some(ref))
    case Event(Msg.Configure(cfg, false), _) => stay()
    case Event(Msg.Configure(cfg, true), d) =>
      val routeCfg = ConfigFactory.parseString(cfg)
      routeTypeToHandlerActorProps(routeId, routeCfg) match {
        case None => transitionTo(States.Failed) using d.copy(cfg = Some(cfg), lastError = Some("Invalid type"))
        case Some(props) => transitionTo(States.Starting) using d.copy(routeHandlerRef = Some(context.watch(context.actorOf(props))))
      }
  }

  onTransition {
    case _ -> States.Started => nextStateData.autoStartWith foreach (_ => self ! IntMsg.Stop("Restarting with new config"))
    case _ -> States.Failed => nextStateData.autoStartWith foreach (self ! _)
    case _ -> States.Stopped => nextStateData.autoStartWith foreach (self ! _)
  }

  onTransition {
    case Started -> _ => for (mRef <- stateData.mediaSub; route <- stateData.eventProcessorHandle) mRef ! MediaSubscriptionActor.Msg.RemoveRoute(route)
    case _ -> Started => for (mRef <- stateData.mediaSub; route <- stateData.eventProcessorHandle) mRef ! MediaSubscriptionActor.Msg.AddRoute(route)
  }

  private def routeTypeToHandlerActorProps(id: String, cfg: Config): Option[Props] =
    config.asOptProps("es.eventProcessorHandle.handler." + id, id, cfg)

  object IntMsg {

    case class Stop(reason: String)

    case class Stopped(stopReason: String)

    case class FailedToStop(stopReason: String, cause: Option[Throwable])

  }

}
