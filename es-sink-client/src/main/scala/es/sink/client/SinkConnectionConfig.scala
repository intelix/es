package es.sink.client


case class SinkConnectionConfig(channel: String,
                                streamId: Int,
                                backpressureStrategy: BackpressureStrategy,
                                maxMessageLength: Option[Int],
                                meta: SinkSessionMeta)
