package es.sink.client

object SinkSessionMeta {
  val HostKey = "host"
  val SourceKey = "src"
  val TagsKey = "tags"
}

case class SinkSessionMeta(tags: String*) {

  import SinkSessionMeta._

  def +(kv: (String, String)): SinkSessionMeta = SinkSessionMeta(tags :+ (kv._1 + "=" + kv._2): _*)

  def host(v: String) = this + (HostKey -> v)

  def source(v: String) = this + (SourceKey -> v)

  def tags(v: String*) = this + (TagsKey -> v.mkString(","))
}

