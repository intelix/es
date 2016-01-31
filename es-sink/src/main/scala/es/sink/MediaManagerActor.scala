package es.sink

import akka.actor.{ActorRef, Props}
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

  raise(Evt.MediaDriverStarted, 'dir -> dir)

  val ctx = new Aeron.Context
  ctx.aeronDirectoryName(dir)
  val aeron = Aeron.connect(ctx)

  val cleaner = context.actorOf(Props[AeronDirectoryCleaner])
  cleaner ! dir

  onMessage {
    case StartSubscription(channel, sId, target) => sender ! SubscriptionRef(context.actorOf(Props(classOf[MediaSubscriptionActor], aeron, channel, sId, target), nameFor(sId)))
  }

  def nameFor(sId: Int) = "stream-" + sId

  override def postStop(): Unit = {
    CloseHelper.quietClose(aeron)
    CloseHelper.quietClose(driver)
    raise(Evt.MediaDriverStopped)
  }

}


object AeronDirectoryCleaner {

  object Evt {
    val SourceId = MediaManagerActor.Evt.SourceId + ".Cleaner"

    case object DirectoryRemoved extends TraceE

    case object UnableToRemoveDirectory extends WarningE

  }

}

private class AeronDirectoryCleaner extends StatelessActor {

  import AeronDirectoryCleaner._

  override val evtSource: EvtSource = Evt.SourceId

  val aeronDirectoryCleanupEnabled = config.asBoolean("sink.aeron.media.auto-cleanup", defaultValue = true)

  val refFileUnused = new java.io.File(SinkClient.ReferenceFilename + ".old")

  var pendingDeletion: List[String] = Try(FileUtils.readLines(refFileUnused).toList).toOption.getOrElse(List())

  if (aeronDirectoryCleanupEnabled) scheduleOnce(5 seconds, Cleanup)

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      deletePending()
    }
  })


  onMessage {
    case dir: String => addToPending(dir)
    case Cleanup =>
      deletePending()
      if (pendingDeletion.nonEmpty) scheduleOnce(30 seconds, Cleanup)
  }

  private def deletePending() = if (aeronDirectoryCleanupEnabled) {
    pendingDeletion = pendingDeletion.filter { case nextDirName =>
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
    storePending()

  }

  private def addToPending(dir: String) = {
    pendingDeletion +:= dir
    storePending()
  }

  private def storePending() = {
    try {
      FileUtils.writeLines(refFileUnused, pendingDeletion)
    } catch {
      case _: Throwable => raise(CommonEvt.EvtWarning, 'msg -> "Unable to write into reference file", 'file -> refFileUnused.getAbsolutePath)
    }
  }

  case object Cleanup

}
