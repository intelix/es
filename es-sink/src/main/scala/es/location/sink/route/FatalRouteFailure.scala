package es.location.sink.route

class FatalRouteFailure (message: String, cause: Throwable) extends RuntimeException(message, cause) with Serializable {
  def this(msg: String) = this(msg, null)
}
