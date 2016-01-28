package es.sink

import akka.actor.{ActorRef, Props}
import es.sink.MediaManagerActor.{SubscriptionRef, StartSubscription}
import org.apache.commons.io.FileUtils
import rs.core.actors.StatelessActor
import rs.core.evt.EvtSource
import uk.co.real_logic.aeron.Aeron
import uk.co.real_logic.aeron.driver.MediaDriver
import uk.co.real_logic.agrona.CloseHelper
import uk.co.real_logic.agrona.concurrent.SigInt

import scala.util.{Failure, Success, Try}

object MediaManagerActor {

  case class StartSubscription(channel: String, streamId: Int, target: ActorRef)
  case class SubscriptionRef(actorRef: ActorRef)
}

class MediaManagerActor() extends StatelessActor {
  override val evtSource: EvtSource = "MediaManager"

  System.setProperty("aeron.dir.delete.on.exit", "True")

  val mediaCtx = new MediaDriver.Context

  var dir = ""
  lazy val refFilename = Option(System.getProperty("es.sink.aeron.dir.reffile")) getOrElse (FileUtils.getTempDirectoryPath + java.io.File.pathSeparator + "aeron_ref")

  val refFile = new java.io.File(refFilename)
  if (refFile.exists() && refFile.isFile) dir = Try(FileUtils.readFileToString(refFile)).getOrElse("")

  val dirAsFile = new java.io.File(dir)
  if (dir != null && !dir.isEmpty && dirAsFile.isDirectory) {
    try {
      FileUtils.deleteDirectory(dirAsFile)
      println(s"!>>> reusing $dir")
      mediaCtx.aeronDirectoryName(dir)
    } catch {
      case _: Throwable =>
    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      println("!>>>> shutdown hook ran")
    }
  })
  SigInt.register(new Runnable {
    override def run(): Unit = {
      println("!>>> SigInt ran")
    }
  })



  val driver = MediaDriver.launchEmbedded(mediaCtx)
  dir = mediaCtx.aeronDirectoryName()


  val ctx = new Aeron.Context
  ctx.aeronDirectoryName(driver.aeronDirectoryName())
  val aeron = Aeron.connect(ctx)

  println("!>>>> Dir: " + dir)
  FileUtils.writeStringToFile(refFile, dir)

  onMessage {
    case StartSubscription(channel, sId, target) => sender ! SubscriptionRef(context.actorOf(Props(classOf[MediaSubscriptionActor], aeron, channel, sId, target), nameFor(sId)))
  }

  def nameFor(sId: Int) = "stream-" + sId

  override def postStop(): Unit = {
    CloseHelper.quietClose(aeron)
    CloseHelper.quietClose(driver)
  }

}
