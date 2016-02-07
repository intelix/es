package es.routes

import rs.core.evt.EvtSource
import rs.core.services.StatelessServiceActor

object RoutesServiceActor {
  object Evt {
    val SourceId = "RoutesService"

  }
}

class RoutesServiceActor(id: String) extends StatelessServiceActor(id) {
  import RoutesServiceActor._

  override val evtSource: EvtSource = Evt.SourceId



}
