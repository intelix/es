package es.sink.client

import java.nio.ByteBuffer

import es.sink.client.BackpressureStrategy.{BlockingRetry, RetryThenDrop, Drop}
import rs.core.utils.NowProvider
import uk.co.real_logic.aeron.{Publication, Aeron}
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

import scala.annotation.tailrec


trait SinkConnection {

  def isConnected: Boolean

  def post(v: String): Boolean

}


private class AeronSinkConnection(cfg: SinkConnectionConfig) extends SinkConnection {

  val Ping = new UnsafeBuffer(ByteBuffer.allocateDirect(1))
  Ping.putByte(0, 'P')

  var buff = new UnsafeBuffer(ByteBuffer.allocateDirect(1024))

  val aeron = Aeron.connect()
  val pub = aeron.addPublication(cfg.channel, cfg.streamId)

  override def isConnected: Boolean = pub.offer(Ping) match {
    case i => i != Publication.NOT_CONNECTED && i != Publication.CLOSED
  }

  override def post(v: String): Boolean = {
    val bs = v.getBytes("UTF-8")
    bs.length match {
      case l if cfg.maxMessageLength.exists(_ < l) => return false
      case l if buff.capacity() < l => buff = new UnsafeBuffer(ByteBuffer.allocateDirect(l))
      case _ =>
    }
    buff.putBytes(0, bs)

    cfg.backpressureStrategy match {
      case RetryThenDrop(_, idle) => idle.reset()
      case BlockingRetry(idle) => idle.reset()
      case _ =>
    }

    offer(bs.length, 0, System.currentTimeMillis())
 }

  @tailrec private def offer(l: Int, attempt: Int, firstAttemptAt: Long): Boolean = pub.offer(buff, 0, l) match {
    case Publication.BACK_PRESSURED | Publication.NOT_CONNECTED =>
      cfg.backpressureStrategy match {
        case RetryThenDrop(retryMs, idle) if retryMs > System.currentTimeMillis() - firstAttemptAt =>
          idle.idle(); offer(l, attempt + 1, firstAttemptAt)
        case BlockingRetry(idle) => idle.idle(); offer(l, attempt + 1, firstAttemptAt)
        case _ => false
      }
  }
}