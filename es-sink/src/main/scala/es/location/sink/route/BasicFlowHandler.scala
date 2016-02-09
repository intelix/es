package es.location.sink.route

import akka.stream.scaladsl._
import com.typesafe.config.Config
import es.model.Payload
import rs.core.evt.EvtSource

import scala.concurrent.Future
import scala.language.{existentials, postfixOps}
import scala.util.{Success, Try}

class BasicFlowHandler(routeId: String, routeCfg: Config) extends AkkaStreamsBasedRouteHandler(routeId, routeCfg) {
  override val evtSource: EvtSource = "TCPRouteHandlerActor"


  override def runFlow(source: Source[Payload, Unit], onError: (Option[Throwable]) => Unit) = {
    Future.successful(source.runForeach(printPayload))
  }

  private def printPayload(p: Payload) = println("!>>>>> PAYLOAD: " + p)
}