package es.sink.client

object SinkSessionMeta {
  val HostKey = "host"
  val SourceKey = "src"
  val TagsKey = "tags"
}

case class SinkSessionMeta(values: (String, String)*) {
  import SinkSessionMeta._
  def +(kv: (String, String)) = SinkSessionMeta(values :+ (kv._1 -> kv._2): _*)
  def host(v: String) = this + (HostKey -> v)
  def source(v: String) = this + (SourceKey -> v)
  def tags(v: String*) = this + (TagsKey -> v.mkString(","))
  private[client] lazy val encode = values.map { case (a,b) => a + "=" + b } mkString "|"
}

