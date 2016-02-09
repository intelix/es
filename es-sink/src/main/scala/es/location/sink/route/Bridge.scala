package es.location.sink.route

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import es.model.Payload
import es.sink.EventProcessor
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.existentials

class Bridge extends Publisher[Payload] with EventProcessor {
  private val subscribers = new AtomicReference[Array[SubscriberSession]](Array())

  //TODO
  val idleStrategy = new BackoffIdleStrategy(100000, 30, MICROSECONDS.toNanos(1), MICROSECONDS.toNanos(100))


  case class SubscriberSession(subscription: Sub, subscriber: Subscriber[_ >: Payload]) {
    def accept(p: Payload): Boolean = if (subscription.isActive && subscription.consumeDemand()) {
      println(s"!>>> Produced next: $p")
      subscriber.onNext(p)
      true
    } else {
      println(s"!>>> Unable to produce, active: ${subscription.isActive}, demand: ${subscription.hasDemand}")
      !subscription.isActive
    }
  }

  override def accept(p: Payload): Boolean =
    if (!super.accept(p)) false
    else {
      println(s"!>>> Received another event.... $p")
      var i = 0
      val current = subscribers.get()
      while (i < current.length) {
        val next = current(i)
        idleStrategy.reset()
        while (!next.accept(p)) idleStrategy.idle()
        i += 1
      }
      true
    }

  class Sub extends Subscription {
    private final val active = new AtomicBoolean(true)
    private final val demand = new AtomicLong(0)

    override def cancel(): Unit = {
      println(s"!>>> Cancelled")
      active.set(false)
    }

    override def request(l: Long): Unit = {
      println(s"!>>>> Requested demand: $l")

      demand.addAndGet(l)
    }

    def isActive = active.get()

    def consumeDemand(): Boolean = if (isActive && demand.get() > 0) {
      demand.decrementAndGet()
      true
    } else false

    def hasDemand = demand.get() > 0

  }


  override def subscribe(subscriber: Subscriber[_ >: Payload]): Unit = {
    println("!>>>> Adding subscriber...")
    val sub = new Sub()
    val session = SubscriberSession(sub, subscriber)
    @tailrec def addToList(): Unit = {
      val current = subscribers.get()
      if (!subscribers.compareAndSet(current, current :+ session)) addToList()
    }
    addToList()
    subscriber.onSubscribe(sub)
  }

}
