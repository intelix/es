package es.model

sealed trait Payload {
  def ts: Long
  def tags: List[String]
}

case class StringPayload(ts: Long, tags: List[String], data: String) extends Payload
case class BinaryPayload(ts: Long, tags: List[String], data: Array[Byte]) extends Payload
