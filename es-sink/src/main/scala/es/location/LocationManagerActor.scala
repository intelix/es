package es.location

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{ActorRef, OneForOneStrategy, Props}
import com.typesafe.config.ConfigFactory
import es.location.media.MediaManagerActor
import es.location.sink.SinkServiceActor
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource
import rs.core.services.StatelessServiceActor
import rs.core.services.internal.InternalMessages.SignalPayload
import rs.core.utils.UUIDTools
import rs.core.{ServiceKey, Subject, TopicKey}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


object LocationManagerActor {

}

class LocationManagerActor(id: String) extends StatelessServiceActor(id) {
  override val evtSource: EvtSource = "LocationManager"


  override def supervisorStrategy = OneForOneStrategy(9999, 5 minutes) {
    case _ => Restart
  }

  val mediaManager = context.actorOf(Props(classOf[MediaManagerActor]))

  var sinks: Map[String, ActorRef] = Map()

  self ! SignalPayload(Subject(ServiceKey(""), TopicKey("addSink"), ""), "", now + 10000000, None)

  onSignal {
    case (Subject(_, TopicKey("addSink"), _), data: String) =>
      Try(ConfigFactory.parseString(data)) match {
        case Success(cfg) =>
          val id = cfg.asString("id", UUIDTools.generateShortUUID)
          sinks += id -> context.actorOf(Props(classOf[SinkServiceActor], id, cfg, mediaManager))
          SignalOk()
        case Failure(_) => SignalFailed()
      }
  }

}
