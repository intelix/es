package es.sink

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{OneForOneStrategy, SupervisorStrategy, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import es.sink.MediaManagerActor.StartSubscription
import rs.core.config.ConfigOps.wrap
import rs.core.services.internal.InternalMessages.SignalPayload
import rs.core.utils.UUIDTools
import rs.core.{ServiceKey, TopicKey, Subject}
import rs.core.evt.EvtSource
import rs.core.services.StatelessServiceActor

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try


object SinkManagerActor {

}

class SinkManagerActor(id: String) extends StatelessServiceActor(id) {
  override val evtSource: EvtSource = "SinkManager"


  override def supervisorStrategy = OneForOneStrategy(9999, 5 minutes) {
    case _ => Restart
  }

  val mediaManager = context.actorOf(Props(classOf[MediaManagerActor], serviceCfg))

  var sinks: Map[String, ActorRef] = Map()

  self ! SignalPayload(Subject(ServiceKey(""), TopicKey("addSink"), ""), "", now + 10000000, None)

  onSignal {
    case (Subject(_, TopicKey("addSink"), _), data: String) =>
      Try(ConfigFactory.parseString(data)).toOption match {
        case Some(cfg) =>
          val id = cfg.asString("id", UUIDTools.generateShortUUID)
          sinks += id -> context.actorOf(Props(classOf[SinkServiceActor], id, cfg, mediaManager))
          SignalOk()
        case None => SignalFailed()
      }
  }

}
