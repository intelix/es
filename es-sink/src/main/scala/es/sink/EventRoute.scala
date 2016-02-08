package es.sink

import com.typesafe.config.Config
import es.model.Payload
import rs.core.config.ConfigOps.wrap

import scala.concurrent.Future
import scala.language.existentials


trait RouteHandle {
  def stop() = {}
}


trait EventProcessor {
  def accept(p: Payload): Boolean = true
}

trait Handle extends EventProcessor {
  def stop(): Future[Boolean]
}

trait WithFilters extends EventProcessor {
  protected val config: Config
  private lazy val filters: Set[String] = config.asString("filter.tags", "").split(",").map(_.trim).filterNot(_.isEmpty).toSet

  private def filtersMatch(p: Payload) = filters.isEmpty || p.tags.exists(filters.contains)

  override def accept(p: Payload): Boolean = if (super.accept(p)) filtersMatch(p) else false
}

trait HandleWithFilters extends Handle with WithFilters

case class ErrorKind(isFatal: Boolean, msg: String, cause: Option[Throwable])
