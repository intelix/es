package es.routes

import rs.core.stream.SetStreamState.SetSpecs
import rs.core.{CompositeTopicKey, Subject, TopicKey}
import rs.core.evt.EvtSource
import rs.core.services.{CompoundStreamIdTemplate, SimpleStreamIdTemplate, StatelessServiceActor}
import rs.core.stream.DictionaryMapStreamState.Dictionary

object RoutesServiceActor {
  object Evt {
    val SourceId = "RoutesService"

  }
}

class RoutesServiceActor(id: String) extends StatelessServiceActor(id) {
  import RoutesServiceActor._

  override val evtSource: EvtSource = Evt.SourceId

  implicit val dict = Dictionary("cfg", "enabled", "criteria")
  implicit val setSpecs = SetSpecs(true)

  object RoutesListStream extends SimpleStreamIdTemplate("routes-list")
  object RouteCfgStream extends CompoundStreamIdTemplate[String]("config")

  onSubjectMapping {
    case Subject(_, TopicKey("routes-list"), _) => RoutesListStream()
    case Subject(_, CompositeTopicKey("config", i), _) => RouteCfgStream(i)
  }

  val tempCfg =
    """
      |type = "tcp"
      |host = "127.0.0.1"
      |port = 10000
    """.stripMargin

  onStreamActive {
    case s@RoutesListStream() => s streamSetSnapshot Set("123")
    case s@RouteCfgStream(i) => s !# Array(tempCfg, true, "some criteria")
  }

}
