package es.location.media.sub

import java.util.concurrent.TimeUnit._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import es.model.{Payload, StringPayload}
import es.sink.EventProcessor
import rs.core.actors.StatelessActor
import rs.core.evt.{EvtSource, InfoE, TraceE}
import uk.co.real_logic.aeron.logbuffer.{FragmentHandler, Header}
import uk.co.real_logic.aeron.{Aeron, FragmentAssembler, Subscription}
import uk.co.real_logic.agrona.concurrent.{BackoffIdleStrategy, IdleStrategy}
import uk.co.real_logic.agrona.{CloseHelper, DirectBuffer}


object MediaSubscriptionActor {

  object Msg {

    case class AddRoute(handle: EventProcessor)

    case class RemoveRoute(handle: EventProcessor)

  }

  private object Internal {

    case class Id(ms: Long, ls: Long) {
      override lazy val toString: String = (1 + ms.signum) + java.lang.Long.toString(ms.abs, 36) + "_" + (1 + ls.signum) + java.lang.Long.toString(ls.abs, 36)
    }

    case class Meta(tsBase: Long, tags: List[String])

  }

  object Evt {

    case object StartingSubscription extends InfoE

    case object NewSource extends InfoE

    case object MetaReceived extends TraceE

    case object DataIgnored extends TraceE

    case object DataReceived extends TraceE

  }

}


class MediaSubscriptionActor(aeron: Aeron, channel: String, streamId: Int) extends StatelessActor {

  import MediaSubscriptionActor._
  import Internal._

  override val evtSource: EvtSource = "MediaSubscription." + streamId

  val subscription = aeron.addSubscription(channel, streamId)
  //  val idleStrategy = new BusySpinIdleStrategy()
  val idleStrategy = new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))

  val running = new AtomicBoolean(true)

  private val router = new AtomicReference[Router](Router())
  var routes = List[EventProcessor]()

  def forwardPayload(p: Payload) = router.get().accept(p)


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
          if (maybeMeta.isEmpty) raise(Evt.NewSource, 'id -> id)
          val tsBase = directBuffer.getLong(offset + 17)
          val seq = directBuffer.getLong(offset + 17 + 8)
          val fieldsLen = directBuffer.getShort(offset + 17 + 8 + 8)
          var i = 0
          var pointer = offset + 17 + 8 + 8 + 2
          var list = List[String]()
          while (i < fieldsLen) {
            val len = directBuffer.getShort(pointer)
            pointer += 2
            val v = cloneDataAsString(directBuffer, pointer, len.toInt)
            pointer += len
            list = v +: list
            i += 1
          }
          val newMeta = Meta(tsBase, list.reverse)
          metas += id -> newMeta
          raise(Evt.MetaReceived, 'id -> id, 'details -> newMeta)
        case 1 if maybeMeta.isDefined =>
          val meta = maybeMeta.get
          val tsShift = directBuffer.getShort(offset + 17)
          val seqPartial = directBuffer.getInt(offset + 17 + 2)
          val ts = meta.tsBase + tsShift
          val str = cloneDataAsString(directBuffer, offset + 17 + 2 + 4, length - 17 - 2 - 4)
          forwardPayload(StringPayload(ts, meta.tags, str))
          raise(Evt.DataReceived, 'id -> id, 'type -> 1, 'contents -> str)
        case 2 if maybeMeta.isDefined =>
          val meta = maybeMeta.get
          val tsShift = directBuffer.getShort(offset + 17)
          val seqPartial = directBuffer.getInt(offset + 17 + 2)
          val ts = meta.tsBase + tsShift
          val tagsLen = directBuffer.getShort(offset + 17 + 2 + 4)
          val tags = cloneDataAsString(directBuffer, offset + 17 + 2 + 4 + 2, tagsLen.toInt)
          val str = cloneDataAsString(directBuffer, offset + 17 + 2 + 4 + 2 + tagsLen, length - (17 + 2 + 4 + 2 + tagsLen))
          var allTags = meta.tags
          tags.split('\t').foreach(allTags +:= _)
          forwardPayload(StringPayload(ts, allTags, str))
          raise(Evt.DataReceived, 'id -> id, 'type -> 2, 'contents -> str, 'tags -> allTags)
        case 1 => raise(Evt.DataIgnored, 'id -> id, 'type -> 1, 'reason -> "Meta pending")
        case 2 => raise(Evt.DataIgnored, 'id -> id, 'type -> 2, 'reason -> "Meta pending")
      }

    }
  })

  val fragmentLimitCount = 256

  val pollingThread = new Thread(new Worker(running, subscription, idleStrategy, fragmentHandler, fragmentLimitCount), "aeron-sub[" + channel + "]:" + streamId)
  pollingThread.setDaemon(true)

  raise(Evt.StartingSubscription, 'channel -> channel, 'streamId -> streamId)

  @throws[Exception](classOf[Exception]) override
  def preStart(): Unit = {
    super.preStart()
    pollingThread.start()
  }

  @throws[Exception](classOf[Exception]) override
  def postStop(): Unit = {
    super.postStop()
    running.set(false)
    CloseHelper.quietClose(subscription)
  }


  onMessage {
    case Msg.AddRoute(r) =>
      log.info(s"!>>> Adding route: $r")
      routes +:= r
      router.set(Router(routes))
    case Msg.RemoveRoute(r) =>
      routes = routes.filter(_ != r)
      router.set(Router(routes))
  }

}

private class Worker(running: AtomicBoolean, sub: Subscription, idleStrategy: IdleStrategy, fragmentHandler: FragmentHandler, fragmentLimitCount: Int) extends Runnable {
  override def run(): Unit = while (running.get()) idleStrategy.idle(sub.poll(fragmentHandler, fragmentLimitCount))
}


private object Router {
  def apply(): Router = new Router(Array())

  def apply(routes: List[EventProcessor]): Router = new Router(routes.toArray)
}

private class Router(routes: Array[EventProcessor]) {
  def accept(p: Payload): Unit = {
    var i = 0
    while (i < routes.length) {
      routes(i).accept(p)
      i += 1
    }
  }
}



