package es.sink.client

import uk.co.real_logic.agrona.concurrent.IdleStrategy

trait BackpressureStrategy

object BackpressureStrategy {
  case object Drop extends BackpressureStrategy
  case class RetryThenDrop(retryMs: Int, idleStrategy: IdleStrategy)
  case class BlockingRetry(idleStrategy: IdleStrategy)
}
