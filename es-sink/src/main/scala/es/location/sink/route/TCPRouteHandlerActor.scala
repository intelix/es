package es.location.sink.route

import akka.actor.{ActorDSL, PoisonPill}
import akka.stream.io.Framing
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.ByteString
import com.typesafe.config.Config
import es.location.sink.route.AkkaStreamsBasedRouteHandler
import es.location.sink.route.AkkaStreamsBasedRouteHandler.{States, Endpoint}
import es.model.{Payload, BinaryPayload, StringPayload}
import rs.core.actors.{ActorState, StatefulActor}
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.{existentials, postfixOps}
import scala.util.{Try, Success}

import TCPRouteHandlerActor._

object TCPRouteHandlerActor {

  case class TCPEndpoint(host: String, port: Int) extends Endpoint

}

class TCPRouteHandlerActor[(routeId: String, routeCfg: Config) extends AkkaStreamsBasedRouteHandler[TCPEndpoint, Tcp.OutgoingConnection](routeId, routeCfg) {
  override val evtSource: EvtSource = "TCPRouteHandlerActor"

  override def parseEndpoints(cfg: Config): Try[List[TCPEndpoint]] =
    Try(List(TCPEndpoint(routeCfg.asOptString("host").get, routeCfg.asOptInt("port").get)))


  override def buildFlow(ep: TCPEndpoint): Flow[Payload, _ >: Any, Future[OutgoingConnection]] = {
    val connection = Tcp().outgoingConnection(ep.host, ep.port)
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
    val mapper = Flow[Payload].map {
      case p: StringPayload =>
        println(s"!>>> Mapping next $p -> ${p.data.getBytes("UTF-8")}")
        ByteString.fromArray(p.data.getBytes("UTF-8"))
      case p: BinaryPayload => ByteString.fromArray(p.data)
    }
    val xx = connection.join(logger.atop(framing))

  }

  override def _buildFlow(ep: TCPEndpoint) = {
    val connection = Tcp().outgoingConnection(ep.host, ep.port)
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
    connection
  }


}