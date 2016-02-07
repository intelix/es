package es.sinkcluster

import es.sinkcluster.SinkClusterService._
import rs.core.{TopicKey, Subject}
import rs.core.evt.EvtSource
import rs.core.services.{CompoundStreamIdTemplate, StreamId, StatelessServiceActor}

object SinkClusterService {
  object Evt {
    val SourceId = "SinkCluster"
  }
}

class SinkClusterService(id: String) extends StatelessServiceActor(id) {
  override val evtSource: EvtSource = Evt.SourceId

  object RegistryStream extends CompoundStreamIdTemplate[String]("service")


  onSubjectMapping {
    case Subject(_, TopicKey("register"), i) => RegistryStream(i)
  }

  onStreamActive {
    case RegistryStream(i) => println("!>>>> Service active: " + i)
  }

  onStreamPassive {
    case RegistryStream(i) => println("!>>>> Service passive: " + i)
  }


}
