package es.sink

import akka.actor.{Stash, ActorRef}
import com.typesafe.config.{ConfigFactory, Config}
import es.model.{StringPayload, Payload}
import es.sink.MediaManagerActor.{SubscriptionRef, StartSubscription}
import es.sink.SinkServiceActor.{Started, Data, Starting}
import rs.core.{Subject, TopicKey}
import rs.core.actors.ActorState
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource
import rs.core.services.StatefulServiceActor

import scala.util.{Success, Try}

object SinkServiceActor {

  case class Data(subRef: Option[ActorRef] = None)

  case object Starting extends ActorState
  case object Started extends ActorState

  private object Internal {
    case object Start
  }

}

class SinkServiceActor(id: String, sinkCfg: Config, mediaManager: ActorRef) extends StatefulServiceActor[Data](id) with Stash {
  override val evtSource: EvtSource = "Sink." + id
  import es.sink.SinkServiceActor.Internal._

//  val channel = sinkCfg.asString("aeron.channel", "udp://localhost:40123")
  val channel = sinkCfg.asString("aeron.channel", "aeron:ipc")
  val streamId = sinkCfg.asInt("aeron.stream-id", 1)

  startWith(Starting, Data())
  self ! Start

  when(Starting) {
    case Event(Start, _) =>
      mediaManager ! StartSubscription(channel, streamId, self)
      stay()
    case Event(SubscriptionRef(ref), data) =>
      transitionTo(Started) using data.copy(subRef = Some(context.watch(ref)))
    case Event(x: AddRoute, _) => stash(); stay()
  }

  var c = 0L
  var last = 0L

  when(Started) {
    case Event(cmd: AddRoute, data) =>
      val route = EventRoute(cmd.cfg)
      data.subRef.foreach(_ ! route)
      stay()
    case Event(StringPayload(ts, meta, data), _) =>
      c += 1

      println(s"!>>> Received [$c]: $meta @ $ts with $data")

/*
      if (c < 2 || c % 100000 == 0 ) {
        val diff = ((System.nanoTime() - last)).toDouble / 100000
        println(s"!>>> Received [$c]: $meta @ $ts with $data, nanos per msg = $diff")
        last = System.nanoTime()
      } */
      stay()
  }

  val tempRouteCfg =
    """
      |type=console
      |filter.tags=t1,t2
    """.stripMargin
  onSignal {
    case (Subject(_, TopicKey("addRoute"), _), s: String) =>
      Try(ConfigFactory.parseString(s)) match {
        case Success(cfg) =>
          self ! AddRoute(cfg)
          SignalOk()
        case _ => SignalFailed()
      }
  }


  case class AddRoute(cfg: Config)

}
