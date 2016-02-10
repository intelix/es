package es.routes

import rs.core._
import rs.core.evt.EvtSource
import rs.core.services.endpoint.Terminal
import rs.core.services._
import rs.core.stream.DictionaryMapStreamState.Dictionary
import rs.core.stream.SetStreamState.SetSpecs

object RoutesServiceActor {

  object Evt {
    val SourceId = "RoutesService"

  }

}

class RoutesServiceActor(id: String) extends StatelessServiceActor(id) with Terminal {

  import RoutesServiceActor._

  override val evtSource: EvtSource = Evt.SourceId

  implicit val dict = Dictionary("cfg", "enabled", "criteria")
  implicit val setSpecs = SetSpecs(true)

  object RoutesListStream extends SimpleStreamIdTemplate("routes-list")

  object RouteCfgStream extends CompoundStreamIdTemplate[String]("config")



  onSubjectMapping {
    case Subject(_, TopicKey("routes-list"), _) => RoutesListStream()
    case Subject(_, CompositeTopicKey("config", i), _) => RouteCfgStream(i)

    case Subject(_, ComplexTopicKey("reversed", Array(a, b)), _) => CompoundStreamId(a, b)
  }

  var routes: Map[String, String] = Map()
  var routesByService: Map[String, Set[String]] = Map()

  onSetRecord {
    case (Subject(ServiceKey(sKey), TopicKey("sampleapp-routes-lists"), _), set) =>
      val myView = routesByService.getOrElse(sKey, Set())
      println(s"!>>> RoutesService received new set: $set, current: $myView")
      val current = set.asInstanceOf[Set[String]]
      (current diff myView).foreach { tKey =>
        println(s"!>>>> Need to subscribe to $tKey with $sKey")
        subscribe(Subject(sKey, CompositeTopicKey("config", tKey)))
      }
      (myView diff current).foreach { tKey => unsubscribeFromExternalRoute(sKey, tKey) }
      routesByService += (sKey -> current)
      publishList()
  }
  onDictMapRecord {
    case (Subject(ServiceKey(sKey), CompositeTopicKey("config", i), _), map) =>
      map.getOpt("cfg").foreach { case cfg: String =>
        val enabled = map.get("enabled", true)
        val applyCriteria = map.getOpt("criteria").asInstanceOf[Option[String]]
        routes += (i -> cfg)
        publishConfig(i)
        publishList()
      }
  }

  def unsubscribeFromExternalRoute(sKey: String, id: String) = {
    unsubscribe(Subject(sKey, CompositeTopicKey("config", id)))
    routes -= id
  }

  onStreamActive {
    case RoutesListStream() => publishList()
    case RouteCfgStream(i) =>
      println(s"!>>>> Want to publish: $i   do I have it: ${routes.contains(i)}")
      publishConfig(i)
    case CompoundStreamId(a, b: String) =>
      println(s"!>>> Auto-subscribing to source at $a , $b")
      subscribe(Subject(a, b))
  }

  def publishList() = {
    println(s"!>>>> WILL PUBLISH: ${routes.keySet}")
    RoutesListStream() streamSetSnapshot Set() ++ routes.keySet
  }

  def publishConfig(i: String) = if (routes.contains(i)) RouteCfgStream(i) !# Array(routes(i), true, "some criteria")

  onStreamPassive {
    case CompoundStreamId(sKey, b: String) =>
      println("!>>> REMOVING")
      unsubscribe(Subject(sKey, b))
      routesByService.get(sKey).foreach(_.foreach { tKey => unsubscribeFromExternalRoute(sKey, tKey) })
      routesByService -= sKey
      publishList()
  }

}
