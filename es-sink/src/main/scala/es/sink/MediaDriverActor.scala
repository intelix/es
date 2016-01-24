package es.sink

import akka.actor.PoisonPill
import rs.core.evt.EvtSource
import rs.core.services.StatelessServiceActor
import uk.co.real_logic.aeron.Aeron
import uk.co.real_logic.aeron.driver.MediaDriver
import uk.co.real_logic.agrona.CloseHelper

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class MediaDriverActor(id: String) extends StatelessServiceActor(id) {
  override val evtSource: EvtSource = "MediaDriver"

  System.setProperty("aeron.dir", "/tmp/aeron")

  val channel = "udp://localhost:40123"
  val streamId = 1

  val driver = MediaDriver.launch()
  val ctx = new Aeron.Context
  ctx.aeronDirectoryName(driver.aeronDirectoryName())
  val aeron = Aeron.connect(ctx)
  val sub = aeron.addSubscription(channel, streamId)

  scheduleOnce(15 seconds, PoisonPill)



  override def postStop(): Unit = {
    CloseHelper.quietClose(sub)
    CloseHelper.quietClose(aeron)
    CloseHelper.quietClose(driver)
    println("!>>> Driver closed")
  }
}
