package es.location.sink

import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import es.location.media.MediaManagerActor.{StartSubscription, SubscriptionRef}
import es.location.sink.route.FlowRouteActor
import es.sink.EventRoute
import rs.core.actors.ActorState
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource
import rs.core.services.StatelessServiceActor
import rs.core.utils.UUIDTools

object SinkServiceActor {

  case class RouteRef(id: String = UUIDTools.generateShortUUID, instance: EventRoute)

  case object Starting extends ActorState

  case object Started extends ActorState

}

class SinkServiceActor(id: String, sinkCfg: Config, mediaManager: ActorRef) extends StatelessServiceActor(id) with RouteSource {
  override val evtSource: EvtSource = "Sink." + id

  implicit val sys = context.system

  //  val channel = sinkCfg.asString("aeron.channel", "udp://localhost:40123")
  val channel = sinkCfg.asString("aeron.channel", "aeron:ipc")
  val streamId = sinkCfg.asInt("aeron.stream-id", 1)

  mediaManager ! StartSubscription(channel, streamId, self)

  subscribeToRoutes()

  var activeRoutes: Map[String, ActorRef] = Map()
  var subscriptionRef: Option[ActorRef] = None

  onActorTerminated { ref =>
    activeRoutes = activeRoutes.filterNot(_._2 == ref)
  }

  onMessage {
    case SubscriptionRef(ref) =>
      subscriptionRef = Some(ref)
      activeRoutes.values.foreach(_ ! FlowRouteActor.Msg.Plug(ref))
    case RouteSourceApi.ApplyRouteConfig(rId, cfg, on, criteria) if isApplicable(criteria) =>
      activeRoutes.getOrElse(rId, {
        val ref = context.watch(context.actorOf(Props(classOf[FlowRouteActor], rId), rId))
        subscriptionRef.foreach(ref ! FlowRouteActor.Msg.Plug(_))
        activeRoutes += rId -> ref
        ref
      }) ! FlowRouteActor.Msg.Configure(cfg, on)
    case x: RouteSourceApi.ApplyRouteConfig => // ignored
    case RouteSourceApi.RemoveRoute(rId) => activeRoutes.get(rId).foreach(_ ! FlowRouteActor.Msg.Delete)
  }

  private def isApplicable(c: Option[String]) = true // TODO

}
