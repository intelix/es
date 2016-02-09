package es.location.sink.route

import akka.actor.{ActorDSL, PoisonPill}
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.Config
import es.location.sink.route.TCPRouteHandlerActor._
import es.model.{BinaryPayload, Payload, StringPayload}
import rs.core.config.ConfigOps.wrap
import rs.core.evt.EvtSource

import scala.concurrent.Future
import scala.language.{existentials, postfixOps}
import scala.util.{Failure, Success, Try}

object TCPRouteHandlerActor {

  case class TCPEndpoint(host: String, port: Int)

}

class TCPRouteHandlerActor(routeId: String, routeCfg: Config) extends AkkaStreamsBasedRouteHandler(routeId, routeCfg) {
  override val evtSource: EvtSource = "TCPRouteHandlerActor"

  def parseEndpoints(cfg: Config): Try[List[TCPEndpoint]] =
    Try(List(TCPEndpoint(routeCfg.asOptString("host").get, routeCfg.asOptInt("port").get)))


  override def runFlow(source: Source[Payload, Unit], onError: (Option[Throwable]) => Unit) =
    Try(TCPEndpoint(routeCfg.asOptString("host").get, routeCfg.asOptInt("port").get)) match {
      case Success(ep) =>
        val connection = Tcp().outgoingConnection(ep.host, ep.port)
        import akka.actor.ActorDSL._

        val ref = ActorDSL.actor(new Act {
          become {
            case x => println(s"!>>> Received: $x")
          }
          whenStopping {
            println(s"!>>> Stopped!")
            onError(None)
          }
        })
        val sink = Sink.actorRef(ref, PoisonPill)
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
        val mapper: Flow[Payload, ByteString, Unit] = Flow[Payload].map {
          case p: StringPayload =>
            println(s"!>>> Mapping next $p -> ${p.data.getBytes("UTF-8")}")
            ByteString.fromArray(p.data.getBytes("UTF-8"))
          case p: BinaryPayload => ByteString.fromArray(p.data)
        }
        val conFlow = connection.join(logger.atop(framing))

        source.via(mapper).viaMat(conFlow)(Keep.right).to(sink).run()
      case Failure(f) => Future.failed(new FatalRouteFailure("Invalid config"))
    }



}