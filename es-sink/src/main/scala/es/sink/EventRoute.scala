package es.sink

import com.typesafe.config.Config
import es.model.Payload
import rs.core.config.ConfigOps.wrap

object EventRoute {
  def apply(cfg: Config): Option[EventRoute] = {
    implicit val c = cfg
    cfg.asOptString("type") map { t=>

      val filters = cfg.asStringList("filters.tags")

      t match {
        case "console" =>
      }

    }
  }
}

trait EventRoute {
  val filters: List[String]
  def accept(p: Payload)
}

private class ConsoleRoute extends EventRoute {
  override def accept(p: Payload): Unit = ???
}
