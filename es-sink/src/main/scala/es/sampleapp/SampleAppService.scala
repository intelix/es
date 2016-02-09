package es.sampleapp

import rs.core.evt.EvtSource
import rs.core.services.endpoint.Terminal
import rs.core.services.{CompoundStreamIdTemplate, SimpleStreamIdTemplate, StatelessServiceActor}
import rs.core.stream.DictionaryMapStreamState.Dictionary
import rs.core.stream.SetStreamState.SetSpecs
import rs.core.{ComplexTopicKey, CompositeTopicKey, Subject, TopicKey}

class SampleAppService(id: String) extends StatelessServiceActor(id) with Terminal {
  override val evtSource: EvtSource = "SampleService"

  implicit val dict = Dictionary("cfg", "enabled", "criteria")
  implicit val setSpecs = SetSpecs(true)

  val tempCfgs = Map(
    "a" ->
      """
        |type = "println"
      """.stripMargin,
    "b" ->
      """
        |type = "tcp"
        |host = "127.0.0.1"
        |port = 11000
      """.stripMargin,
    "c" ->
      """
        |type = "tcp"
        |host = "127.0.0.1"
        |port = 11001
      """.stripMargin)

  object RoutesListStream extends SimpleStreamIdTemplate("routes-list")

  object RouteCfgStream extends CompoundStreamIdTemplate[String]("config")

  subscribe(Subject("es-routes", ComplexTopicKey("reversed", id, "routes-lists")))

  onSubjectMapping {
    case Subject(_, TopicKey("routes-lists"), _) => RoutesListStream()
    case Subject(_, CompositeTopicKey("config", i), _) => RouteCfgStream(i)
  }

  onStreamActive {
    case s@RoutesListStream() => s streamSetSnapshot Set("b")
    case s@RouteCfgStream(i) => s !# Array(tempCfgs(i), true, "some criteria")
  }

}
