package es.sink.client


object SinkClient {
  def apply() = new AeronSinkClient
}

trait SinkClient {
  def connect(channel: String, streamId: Int, backpressureStrategy: BackpressureStrategy, meta: SinkSessionMeta): SinkConnection
}

class AeronSinkClient extends SinkClient{
  override def connect(channel: String, streamId: Int, backpressureStrategy: BackpressureStrategy, meta: SinkSessionMeta): SinkConnection =
    new AeronSinkConnection(channel, streamId, backpressureStrategy, meta)
}