package es.sink.client

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import es.sink.client.BackpressureStrategy.{BlockingRetry, RetryThenDrop}
import uk.co.real_logic.aeron.Aeron
import uk.co.real_logic.aeron.Publication._
import uk.co.real_logic.agrona.{ErrorHandler, CloseHelper, DirectBuffer}
import uk.co.real_logic.agrona.concurrent.{SigInt, UnsafeBuffer}

import scala.annotation.tailrec


object SinkConnection {

  val PostSucceeded = 0
  val PostFailedIllegalTag = -1
  val PostFailedTagsTooLong = -2
  val PostFailedMsgTooLong = -3
  val PostFailedBackPressured = -4
  val PostFailedNotConnected = -5
  val PostFailedOther = -6

}


trait SinkConnection extends AutoCloseable {

  def post(v: String): Int

  def postWithTags(v: String, tags: String*): Int

  //  def postBinary(v: Array[Byte]): Int
  //
  //  def postBinaryWithTags(v: Array[Byte], tags: String*): Int

}


private[client] class AeronSinkConnection(cfg: SinkConnectionConfig, proxy: PublicationProxy) extends SinkConnection {

  import SinkConnection._

  val IdHeaderSize = 16
  val MsgTypeSize = 1

  val MetaHeaderSize = 8
  val DataHeaderSize = 2

  val PayloadIdx = IdHeaderSize + MsgTypeSize

  val MetaPayloadIdx = PayloadIdx + MetaHeaderSize
  val DataPayloadIdx = PayloadIdx + DataHeaderSize

  val id = {
    val uuid = UUID.randomUUID()
    val bytes = new UnsafeBuffer(ByteBuffer.allocateDirect(IdHeaderSize))
    bytes.putLong(0, uuid.getLeastSignificantBits)
    bytes.putLong(8, uuid.getMostSignificantBits)
    bytes
  }

  val MetaBuffer = {
    val asBytesList = cfg.meta.tags.map(_.getBytes("UTF-8"))
    val ArrayLenSize = 2
    val metaBytesLen = asBytesList.foldLeft(ArrayLenSize) { case (len, t) => len + 2 + t.length }
    val b = allocWithId(metaBytesLen + MetaPayloadIdx)
    putMsgType(0, b)
    b.putShort(MetaPayloadIdx, asBytesList.length.toShort)
    asBytesList.foldLeft(MetaPayloadIdx + ArrayLenSize) {
      case (idx, t) =>
        b.putShort(idx, t.length.toShort)
        b.putBytes(idx + 2, t)
        idx + 2 + t.length
    }
    b
  }

  private def allocWithId(len: Int) = {
    val buff = new UnsafeBuffer(ByteBuffer.allocateDirect(len))
    buff.putBytes(0, id, 0, IdHeaderSize)
    buff
  }

  private def putMsgType(t: Byte, buff: UnsafeBuffer) = buff.putByte(IdHeaderSize, t)

  var buff = allocWithId(1024)

  SigInt.register(new Runnable {
    override def run(): Unit = {
      close()
    }
  })

  var metaPublishedAt = 0L

  val DataWindowMs = 30000 // value should not exceed Short.MaxValue

  private def shouldPublishMeta = System.currentTimeMillis() - metaPublishedAt > DataWindowMs


  def meta(ts: Long) = {
    MetaBuffer.putLong(IdHeaderSize + MsgTypeSize, ts)
    MetaBuffer
  }

  private def tsShift(tsBase: Long): Short = (System.currentTimeMillis() - tsBase).toShort

  override def post(v: String): Int = {
    offerMeta() match {
      case (PostSucceeded, tsBase) =>
        val bs = v.getBytes("UTF-8")
        val payloadLen = bs.length
        if (msgIsTooLong(payloadLen)) return PostFailedMsgTooLong
        val buff = bufferOfDim(payloadLen + DataHeaderSize)
        putMsgType(1, buff)
        buff.putShort(PayloadIdx, tsShift(tsBase))
        buff.putBytes(DataPayloadIdx, bs)
        offerData(buff, payloadLen + DataPayloadIdx)
      case (res, _) => res
    }
  }

  override def postWithTags(v: String, tags: String*): Int = {
    offerMeta() match {
      case (PostSucceeded, tsBase) =>
        if (tagsContainIllegalChar(tags)) return PostFailedIllegalTag
        val tagsBs = tagsToBytes(tags)
        if (tagsBs.length > Short.MaxValue) return PostFailedTagsTooLong
        val bs = v.getBytes("UTF-8")
        val payloadLen = 2 + tagsBs.length + bs.length
        if (msgIsTooLong(payloadLen)) return PostFailedMsgTooLong
        val buff = bufferOfDim(payloadLen + DataHeaderSize)
        putMsgType(2, buff)
        buff.putShort(PayloadIdx, tsShift(tsBase))
        buff.putShort(DataPayloadIdx, tagsBs.length.toShort)
        buff.putBytes(DataPayloadIdx + 2, tagsBs)
        buff.putBytes(DataPayloadIdx + 2 + tagsBs.length, bs)
        offerData(buff, payloadLen + DataPayloadIdx)
      case (res, _) => res
    }
  }

  private def tagsContainIllegalChar(tags: Seq[String]) = tags.exists(_.indexOf('\t') > -1)

  private def tagsToBytes(tags: Seq[String]) = tags.mkString("\t").getBytes("UTF-8")

  private def bufferOfDim(size: Int) = {
    if (buff.capacity() < size + PayloadIdx) buff = allocWithId(size + PayloadIdx)
    buff
  }

  private def msgIsTooLong(msgSize: Int) = cfg.maxMessageLength.exists(_ < msgSize)

  private def resetIdleStrategy() = cfg.backpressureStrategy match {
    case RetryThenDrop(_, idle) => idle.reset()
    case BlockingRetry(idle) => idle.reset()
    case _ =>
  }

  private def offerData(buff: DirectBuffer, len: Int): Int = {
    resetIdleStrategy()
    offer(buff, len, System.currentTimeMillis())
  }

  private def canRetry(firstAttemptAt: Long) = cfg.backpressureStrategy match {
    case RetryThenDrop(retryMs, idle) if retryMs > System.currentTimeMillis() - firstAttemptAt => idle.idle(); true
    case BlockingRetry(idle) => idle.idle(); true
    case _ => false
  }

  private def offerMeta(): (Int, Long) = {
    if (shouldPublishMeta) {
      resetIdleStrategy()
      val now = System.currentTimeMillis()
      val metaBuff = meta(now)
      offer(metaBuff, metaBuff.capacity(), now) match {
        case PostSucceeded =>
          metaPublishedAt = now
          (PostSucceeded, metaPublishedAt)
        case x =>
          metaPublishedAt = 0L
          (x, metaPublishedAt)
      }
    } else (PostSucceeded, metaPublishedAt)
  }


  @tailrec private def offer(source: DirectBuffer, len: Int, firstAttemptAt: Long): Int =
    proxy.offer(source, 0, len) match {
      case BACK_PRESSURED | NOT_CONNECTED | ADMIN_ACTION | CLOSED if canRetry(firstAttemptAt) => offer(source, len, firstAttemptAt)
      case BACK_PRESSURED => PostFailedBackPressured
      case NOT_CONNECTED => PostFailedNotConnected
      case ADMIN_ACTION | CLOSED => PostFailedOther
      case _ => PostSucceeded
    }

  override def close(): Unit = proxy.close()

}