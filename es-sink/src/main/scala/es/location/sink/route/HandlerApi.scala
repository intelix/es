package es.location.sink.route

import es.sink.EventProcessor


object HandlerApi {

  case object Pending
  case class Started(ep: EventProcessor)

}
