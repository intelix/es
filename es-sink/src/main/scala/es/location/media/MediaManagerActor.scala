package es.location.media

import akka.actor.{ActorRef, Props}
import es.location.media.MediaManagerActor.SubscriptionRef
import es.location.media.sub.MediaSubscriptionActor
import es.sink.client.SinkClient
import org.apache.commons.io.FileUtils
import rs.core.actors.StatelessActor
import rs.core.config.ConfigOps.wrap
import rs.core.evt._
import uk.co.real_logic.aeron.Aeron
import uk.co.real_logic.aeron.driver.MediaDriver
import uk.co.real_logic.agrona.CloseHelper

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object MediaManagerActor {

  case class StartSubscription(channel: String, streamId: Int, target: ActorRef)

  case class SubscriptionRef(actorRef: ActorRef)

  object Evt {
    val SourceId = "MediaManager"

    case object MediaDriverStarted extends InfoE

    case object MediaDriverStopped extends InfoE

    case object DirectoryRemoved extends TraceE

    case object UnableToRemoveDirectory extends TraceE

  }


}

class MediaManagerActor() extends StatelessActor {

  import MediaManagerActor._

  override val evtSource: EvtSource = Evt.SourceId
  addEvtFields('kind -> "Aeron")

  lazy val refFile = new java.io.File(SinkClient.ReferenceFilename)

  val aeronDirectory = config.asOptString("sink.media.aeron.directory")

  val mediaCtx = new MediaDriver.Context
  aeronDirectory.foreach(mediaCtx.aeronDirectoryName)

  val driver = MediaDriver.launchEmbedded(mediaCtx)
  val dir = mediaCtx.aeronDirectoryName()

  var liveSubscriptions = Map[SubscriptionId, ActorRef]()

  raise(Evt.MediaDriverStarted, 'dir -> dir)

  val ctx = new Aeron.Context
  ctx.aeronDirectoryName(dir)
  val aeron = Aeron.connect(ctx)
  val aeronDirectoryCleanupEnabled = config.asBoolean("sink.aeron.media.auto-cleanup", defaultValue = true)
  val refFileUnused = new java.io.File(SinkClient.ReferenceFilename + ".old")
  storePending(deletePending(Try(FileUtils.readLines(refFileUnused).toList).toOption.getOrElse(List())) :+ dir)

  try {
    FileUtils.writeStringToFile(refFile, dir)
  } catch {
    case _: Throwable => raise(CommonEvt.EvtError, 'msg -> s"Unable to write to: ${SinkClient.ReferenceFilename}")
  }

  onMessage {
    case StartSubscription(channel, sId, target) =>
      val key = SubscriptionId(channel, sId)
      sender ! SubscriptionRef(liveSubscriptions.getOrElse(key, {
        val ref = context.actorOf(Props(classOf[MediaSubscriptionActor], aeron, channel, sId), nameFor(sId))
        liveSubscriptions += key -> ref
        println(s"!>>> Started $key -> $ref")
        ref
      }))
  }

  def nameFor(sId: Int) = "stream-" + sId

  override def postStop(): Unit = {
    CloseHelper.quietClose(aeron)
    CloseHelper.quietClose(driver)
    raise(Evt.MediaDriverStopped)
  }

  private def storePending(list: List[String]) = {
    try {
      FileUtils.writeLines(refFileUnused, list)
    } catch {
      case _: Throwable => raise(CommonEvt.EvtWarning, 'msg -> s"Unable to write to: ${refFileUnused.getAbsolutePath}")
    }
  }
  private def deletePending(list: List[String]): List[String] =
    if (!aeronDirectoryCleanupEnabled) list else list.filter { case nextDirName =>
      val fd = new java.io.File(nextDirName)
      if (fd.isDirectory) {
        try {
          FileUtils.deleteDirectory(fd)
          raise(Evt.DirectoryRemoved, 'dir -> nextDirName)
          false
        } catch {
          case _: Throwable =>
            raise(Evt.UnableToRemoveDirectory, 'dir -> nextDirName)
            true
        }
      } else false
    }

  case object Cleanup



  case class SubscriptionId(channel: String, streamId: Int)

}



