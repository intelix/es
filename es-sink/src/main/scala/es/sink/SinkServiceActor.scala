package es.sink

import es.model.Payload
import rs.core.evt.EvtSource
import rs.core.services.{StatelessServiceActor, StatefulServiceActor}
import rs.core.stream.DictionaryMapStreamState.Dictionary


object SinkServiceActor {

}

class SinkServiceActor(id: String) extends StatelessServiceActor(id) {
  override val evtSource: EvtSource = "Sink"

  val dict = Dictionary("summary")


  onMessage {
    case t: Payload =>
  }



}
