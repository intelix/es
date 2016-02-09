package es.location.sink

import rs.core.services.BaseServiceActor
import rs.core.services.endpoint.Terminal
import rs.core.{CompositeTopicKey, ServiceKey, Subject, TopicKey}

trait RouteSource extends Terminal {
  this: BaseServiceActor =>

  protected def subscribeToRoutes() = {
    subscribe(Subject("es-routes", "routes-list"))
  }

  private var activeRouteIds: Set[String] = Set()

  onSetRecord {
    case (Subject(ServiceKey("es-routes"), TopicKey("routes-list"), _), set) =>
      val current = set.map(_.toString)
      println(s"!>>>> Received new list: $set, current: $current")
      (current diff activeRouteIds).foreach(add)
      (activeRouteIds diff current).foreach(remove)
  }
  onDictMapRecord {
    case (Subject(ServiceKey("es-routes"), CompositeTopicKey("config", i), _), map) =>
      map.getOpt("cfg").foreach { case cfg: String =>
        val enabled = map.get("enabled", true)
        val applyCriteria = map.getOpt("criteria").asInstanceOf[Option[String]]
        self ! RouteSourceApi.ApplyRouteConfig(i, cfg, enabled, applyCriteria)
      }
  }

  private def add(id: String) = {
    subscribeToRouteConfig(id)
    println(s"!>>> Subscribed to $id")
    activeRouteIds += id
  }

  private def remove(id: String) = {
    println(s"!>>> Unsubscribed from $id")
    unsubscribeFromRouteConfig(id)
    activeRouteIds -= id
    self ! RouteSourceApi.RemoveRoute(id)
  }

  private def subscribeToRouteConfig(id: String) = subscribe(routeConfigSubject(id))

  private def unsubscribeFromRouteConfig(id: String) = unsubscribe(routeConfigSubject(id))

  private def routeConfigSubject(id: String) = Subject("es-routes", CompositeTopicKey("config", id))


  object RouteSourceApi {

    case class ApplyRouteConfig(id: String, cfg: String, enabled: Boolean, criteria: Option[String])

    case class RemoveRoute(id: String)

  }

}
