package es.sinkcluster

import rs.core.Subject
import rs.core.evt.EvtSource
import rs.core.services.StatelessServiceActor
import rs.core.services.endpoint.Terminal


class SampleService(id: String) extends StatelessServiceActor(id) with Terminal {
  override val evtSource: EvtSource = "Sample"

  subscribe(Subject("sink-cluster", "register", id))

}
