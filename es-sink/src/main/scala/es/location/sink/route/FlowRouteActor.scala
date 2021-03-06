package es.location.sink.route

import java.util.concurrent.atomic.AtomicReference

import akka.actor.{FSM, ActorRef, Props, Terminated}
import com.typesafe.config.{Config, ConfigFactory}
import es.location.media.sub.MediaSubscriptionActor
import es.location.sink.SinkServiceActor.Started
import es.location.sink.route.FlowRouteActor.Data
import es.model.Payload
import es.sink.EventProcessor
import rs.core.actors.{ActorState, StatefulActor}
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource

object FlowRouteActor {

  object Msg {

    case class Configure(cfg: String, enabled: Boolean)

    case class Plug(ref: ActorRef)

    case object Delete

    case object TerminateRoute

  }

  case class Data(
                   mediaSub: Option[ActorRef],
                   cfg: Option[String] = None,
                   routeHandlerRef: Option[ActorRef] = None,
                   eventProcessorProxy: Option[Processor] = None,
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


class Processor extends EventProcessor {

  private val ref = new AtomicReference[EventProcessor]()

  override def accept(p: Payload): Boolean = {
    val r = ref.get()
    if ( r != null ) r.accept(p) else true
  }

  private[route] def linkWith(p: EventProcessor) = ref.set(p)
  private[route] def unlink() = ref.set(null)

}

class FlowRouteActor(routeId: String) extends StatefulActor[Data] {

  println(s"!>>> Started FlowRouteActor : $routeId")

  import FlowRouteActor._

  override val evtSource: EvtSource = Evt.SourceId + "." + routeId

  startWith(States.Unconfigured, Data(mediaSub = None))

  when(States.Unconfigured)(PartialFunction.empty)
  when(States.Failed)(PartialFunction.empty)
  when(States.Stopped)(PartialFunction.empty)

  when(States.Starting) {
    case Event(m: Msg.Configure, d) =>
      d.routeHandlerRef.foreach(_ ! Msg.TerminateRoute)
      stay() using d.copy(autoStartWith = Some(m))
    case Event(Terminated(ref), d) if d.routeHandlerRef.contains(ref) && d.autoStartWith.isDefined => transitionTo(States.Stopped) using d.copy(routeHandlerRef = None)
    case Event(Terminated(ref), d) if d.routeHandlerRef.contains(ref) =>
      println(s"!>>> Terminated: $ref")
      transitionTo(States.Failed) using d.copy(routeHandlerRef = None)
    case Event(HandlerApi.Started(h), d) =>
      d.eventProcessorProxy.foreach(_.linkWith(h))
      transitionTo(States.Started)
  }

  when(States.Started) {
    case Event(Msg.Plug(ref), d) =>
      d.eventProcessorProxy.foreach(ref ! MediaSubscriptionActor.Msg.AddRoute(_))
      stay() using d.copy(mediaSub = Some(ref))
    case Event(IntMsg.Stop(r), d) =>
      d.routeHandlerRef.foreach(_ ! Msg.TerminateRoute)
      transitionTo(States.Stopping)
    case Event(HandlerApi.Pending, d) =>
      d.eventProcessorProxy.foreach(_.unlink())
      transitionTo(States.Starting)
  }

  when(States.Stopping) {
    case Event(m: Msg.Configure, d) => stay() using d.copy(autoStartWith = Some(m))
    case Event(IntMsg.Stopped(r), d) =>
      d.eventProcessorProxy.foreach(_.unlink())
      transitionTo(States.Stopped) using d.copy(lastError = None)
    case Event(IntMsg.FailedToStop(r, f), d) =>
      println(s"!>>> FailedToStop: $r $f")

      transitionTo(States.Failed) using d.copy(lastError = Some(f.map(_.getMessage).getOrElse("Unable to stop")))
  }

  otherwise {
    case Event(Msg.Plug(ref), d) =>
      stay() using d.copy(mediaSub = Some(ref))
    case Event(Msg.Configure(cfg, false), _) => stay()
    case Event(Msg.Configure(cfg, true), d) =>
      val routeCfg = ConfigFactory.parseString(cfg)
      routeTypeToHandlerActorProps(routeId, routeCfg) match {
        case None =>
          println(s"!>>> bad cfg: $routeId [$routeCfg]")

          transitionTo(States.Failed) using d.copy(cfg = Some(cfg), lastError = Some("Invalid type"))
        case Some(props) => transitionTo(States.Starting) using d.copy(routeHandlerRef = Some(context.watch(context.actorOf(props))), eventProcessorProxy = Some(new Processor))
      }
    case Event(Msg.Delete, d) =>
      println("!>>> STOPPING")
      d.routeHandlerRef.foreach(context.stop)
      unlink(d)
      stop(FSM.Normal)
  }

  onTransition {
    case _ -> States.Started => nextStateData.autoStartWith foreach (_ => self ! IntMsg.Stop("Restarting with new config"))
    case _ -> States.Failed => nextStateData.autoStartWith foreach (self ! _)
    case _ -> States.Stopped => nextStateData.autoStartWith foreach (self ! _)
  }

  onTransition {
    case States.Started -> _ => unlink(stateData)
    case _ -> States.Started => for (mRef <- nextStateData.mediaSub; route <- nextStateData.eventProcessorProxy) mRef ! MediaSubscriptionActor.Msg.AddRoute(route)
  }

  private def unlink(stateData: Data) = for (mRef <- stateData.mediaSub; route <- stateData.eventProcessorProxy) mRef ! MediaSubscriptionActor.Msg.RemoveRoute(route)

  private def routeTypeToHandlerActorProps(id: String, cfg: Config): Option[Props] =
    cfg.asOptString("type").flatMap { t => config.asOptProps("es.eventProcessorHandle.handler." + t, id, cfg) }


  object IntMsg {

    case class Stop(reason: String)

    case class Stopped(stopReason: String)

    case class FailedToStop(stopReason: String, cause: Option[Throwable])

  }

}
