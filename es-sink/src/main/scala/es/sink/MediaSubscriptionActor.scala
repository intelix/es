package es.sink

import java.util.UUID
import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorRef
import es.model.{StringPayload, Payload}
import rs.core.actors.StatelessActor
import rs.core.evt.EvtSource
import uk.co.real_logic.aeron.logbuffer.{FragmentHandler, Header}
import uk.co.real_logic.aeron.{Aeron, FragmentAssembler, Subscription}
import uk.co.real_logic.agrona.concurrent.{BusySpinIdleStrategy, BackoffIdleStrategy, IdleStrategy}
import uk.co.real_logic.agrona.{CloseHelper, DirectBuffer}


case class Id(ms: Long, ls: Long)
case class Meta(tsBase: Long, tags: List[String])

class MediaSubscriptionActor(aeron: Aeron, channel: String, streamId: Int, target: ActorRef) extends StatelessActor {
  override val evtSource: EvtSource = "MediaSubscription." + streamId

  val subscription = aeron.addSubscription(channel, streamId)
//  val idleStrategy = new BusySpinIdleStrategy()
  val idleStrategy = new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))

  val running = new AtomicBoolean(true)


  private def cloneData(directBuffer: DirectBuffer, offset: Int, length: Int) = {
    val data = Array.ofDim[Byte](length)
    directBuffer.getBytes(offset, data)
    data
  }
  private def cloneDataAsString(directBuffer: DirectBuffer, offset: Int, length: Int) = new String(cloneData(directBuffer, offset, length), "UTF-8")

  val fragmentHandler = new FragmentAssembler(new FragmentHandler {

    var metas: Map[Id, Meta] = Map()

    override def onFragment(directBuffer: DirectBuffer, offset: Int, length: Int, header: Header): Unit = {
      if (length < 16 + 1) return

      val id = Id(directBuffer.getLong(offset), directBuffer.getLong(offset + 8))
      val msgType = directBuffer.getByte(offset + 16)
      val maybeMeta = metas.get(id)

      msgType match {
        case 0 =>
          if (maybeMeta.isEmpty) {
            println(s"!>>> New Id: $id")
          }
          val tsBase = directBuffer.getLong(offset + 17)
          val fieldsLen = directBuffer.getShort(offset + 17 + 8)
          var i = 0
          var pointer = offset + 17 + 8 + 2
          var list = List[String]()
          while (i< fieldsLen) {

            val len = directBuffer.getShort(pointer)
            pointer += 2
            val v = cloneDataAsString(directBuffer, pointer, len.toInt)
            pointer += len

            list = v +: list

            i += 1
          }

          val newMeta = Meta(tsBase, list.reverse)
          metas += id -> newMeta
          println(s"!>>>> Received meta: $newMeta")
        case 1 if maybeMeta.isDefined =>
          val meta = maybeMeta.get
          val tsShift = directBuffer.getShort(offset + 17)
          val ts = meta.tsBase + tsShift
          val str = cloneDataAsString(directBuffer, offset + 17 + 2, length - 17 - 2)
          target ! StringPayload(ts, meta.tags, str)
        case 2 if maybeMeta.isDefined =>
          val meta = maybeMeta.get
          val tsShift = directBuffer.getShort(offset + 17)
          val ts = meta.tsBase + tsShift
          val tagsLen = directBuffer.getShort(offset + 17 + 2)
          val tags = cloneDataAsString(directBuffer, offset + 17 + 2 + 2, tagsLen.toInt)
          val str = cloneDataAsString(directBuffer, offset + 17 + 2 + 2 + tagsLen, length - (17 + 2 + 2 + tagsLen))
          var allTags = meta.tags
          tags.split('\t').foreach(allTags +:= _)
          target ! StringPayload(ts, allTags, str)
        case 1  => println(s"!>>> Received update for $id but have no meta - ignored")
      }

    }
  })

  val fragmentLimitCount = 256

  val pollingThread = new Thread(new Worker(running, subscription, idleStrategy, fragmentHandler, fragmentLimitCount), "aeron-sub[" + channel + "]:" + streamId)
  pollingThread.setDaemon(true)

  println(s"!>>>> Starting subscription $channel, $streamId")

  @throws[Exception](classOf[Exception]) override
  def preStart(): Unit = pollingThread.start()

  @throws[Exception](classOf[Exception]) override
  def postStop(): Unit = {
    running.set(false)
    CloseHelper.quietClose(subscription)
  }

}

private class Worker(running: AtomicBoolean, sub: Subscription, idleStrategy: IdleStrategy, fragmentHandler: FragmentHandler, fragmentLimitCount: Int) extends Runnable {
  override def run(): Unit = while (running.get()) idleStrategy.idle(sub.poll(fragmentHandler, fragmentLimitCount))
}
