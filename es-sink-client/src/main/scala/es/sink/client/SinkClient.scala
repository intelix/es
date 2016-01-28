package es.sink.client

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.Logger
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import uk.co.real_logic.aeron.{Aeron, Publication}
import uk.co.real_logic.agrona.{CloseHelper, DirectBuffer, ErrorHandler}

import scala.util.Try


class PublicationProxy(val channel: String, val streamId: Int) {
  val logger: Logger = Logger(LoggerFactory.getLogger("es.sink.client.publication"))

  @volatile private var publication: Publication = null
  @volatile var closed = false

  def set(p: Publication) =
    if (!closed) {
      publication = p
      logger.debug(s"Publication confirmed for $channel stream $streamId")
    } else {
      logger.debug(s"Publication confirmed for $channel stream $streamId - no longer needed")
      p.close()
    }

  def offer(buff: DirectBuffer, offset: Int, len: Int): Long =
    if (publication == null) Publication.NOT_CONNECTED else publication.offer(buff, offset, len)

  def offer(buff: DirectBuffer): Long =
    if (publication == null) Publication.NOT_CONNECTED else publication.offer(buff)

  def close() = {
    closePublication()
    logger.debug(s"Publication is now closed for $channel stream $streamId")
    closed = true
  }

  private[client] def closePublication() = {
    if (publication != null) CloseHelper.quietClose(publication)
  }
}


object SinkClient {

  lazy val refFilename = Option(System.getProperty("es.sink.aeron.dir.reffile")) getOrElse (FileUtils.getTempDirectoryPath + java.io.File.pathSeparator + "aeron_ref")

  lazy val client: SinkClient = new AeronSinkClient

  def apply(): SinkClient = client
}

trait SinkClient extends AutoCloseable {
  def connect(cfg: SinkConnectionConfig): SinkConnection

  def close(): Unit
}

private class AeronSinkClient extends SinkClient {

  val logger: Logger = Logger(LoggerFactory.getLogger("es.sink.client"))


  val reconnectRequired = new AtomicBoolean(false)
  val running = new AtomicBoolean(true)


  val pending = new LinkedBlockingQueue[Any]()

  val reconnectAttemptInterval = Option(System.getProperty("es.sink.aeron.reconnect.interval.ms")).getOrElse("1000").toInt

  new Thread(new Runnable {

    var activePublications: List[PublicationProxy] = List()

    override def run(): Unit = {
      logger.debug(s"Aeron Connection Manager started")
      logger.info(s"Using reference file (use -Des.sink.aeron.dir.reffile): ${SinkClient.refFilename}")

      var lastKnownDirectory: Option[String] = None

      while (running.get()) {
        reconnectRequired.set(false)
        val aeronCtx = new Aeron.Context
        aeronCtx.errorHandler(new ErrorHandler {
          override def onError(throwable: Throwable): Unit = {
            logger.warn("Aeron driver failure", throwable)
            reconnectRequired.set(true)
          }
        })

        var dir = ""
        val refFile = new java.io.File(SinkClient.refFilename)
        if (refFile.exists() && refFile.isFile) dir = Try(FileUtils.readFileToString(refFile)).getOrElse("")

        if (dir != null && !dir.isEmpty && new java.io.File(dir).isDirectory) {

          aeronCtx.aeronDirectoryName(dir)

          if (!lastKnownDirectory.contains(dir)) {
            lastKnownDirectory = Some(dir)
            logger.info(s"Aeron directory changed to: $dir")
          }

          logger.debug("Connecting to Aeron driver")
          val aeron = Aeron.connect(aeronCtx)
          logger.info("Connected to Aeron driver")
          activePublications.foreach(pending.add)
          activePublications = List()
          while (running.get() && !reconnectRequired.get()) pending.poll(1, TimeUnit.SECONDS) match {
            case null =>
            case c: PublicationProxy if !c.closed =>
              logger.debug(s"Adding publication for ${c.channel} stream ${c.streamId}")
              try {
                c.set(aeron.addPublication(c.channel, c.streamId))
                logger.info(s"Publication added for ${c.channel} stream ${c.streamId}")
              } catch {
                case x: Throwable =>
                  logger.warn(s"Unable to add publication for ${c.channel} stream ${c.streamId}")
                  reconnectRequired.set(true)
              }
              activePublications +:= c
            case _ =>
          }
          logger.info("Closing connection to Aeron driver")
          activePublications.foreach(_.closePublication())
          aeron.close()
          logger.info("Closed connection to Aeron driver")
        }

        if (reconnectAttemptInterval > 0) Thread.sleep(reconnectAttemptInterval.toLong)
      }
    }
  }) {
    setDaemon(true)
    setName("Aeron Connection Manager")
    start()
  }

  override def connect(cfg: SinkConnectionConfig): SinkConnection = {
    val proxy = new PublicationProxy(cfg.channel, cfg.streamId)
    pending.add(proxy)
    new AeronSinkConnection(cfg, proxy)
  }

  override def close(): Unit = running.set(false)
}