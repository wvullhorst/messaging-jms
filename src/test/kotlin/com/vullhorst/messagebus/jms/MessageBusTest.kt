package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.runAfterDelay
import com.vullhorst.messagebus.jms.model.*
import mu.KotlinLogging
import org.apache.activemq.ActiveMQConnectionFactory
import org.junit.Test
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

class MessageBusTest {
    companion object {
        private val factory = ActiveMQConnectionFactory("tcp://syn1:61616")
        private val messageBus = messageBus()
        private fun messageBus(): MessageBus<Event> {
            logger.info("creating message bus")
            factory.clientID = "testbus"
            return MessageBus(factory::createConnection,
                    { session, event -> event.serialize(session) },
                    { message -> message.deserialize() })
        }
    }

    private val topic = Channel.Topic("testtopic", "topicconsumer")
    private val queue = Channel.Queue("testqueue")
    private val simpleEvent = Event(EventId("event1"), Date())

    @Test
    fun eventBusCanSendAndReceiveEventsViaTopic() {
        val nMessages = 10
        val latch = CountDownLatch(nMessages)
        messageBus.receive(topic) {
            latch.countDown()
            Try.just(Unit)
        }
        runAfterDelay(1, TimeUnit.SECONDS) {
            for (i in 1..nMessages) {
                messageBus.send(topic, simpleEvent)
            }
            latch.await(10, TimeUnit.SECONDS)
            logger.info("done")
        }
    }

    @Test
    fun eventBusCanSendAndReceiveEventsViaQueue() {
        val nMessages = 10
        val latch = CountDownLatch(nMessages)
        messageBus.receive(queue) {
            println("message received")
            latch.countDown()
            Try.just(Unit)
        }
        runAfterDelay(1, TimeUnit.SECONDS) {
            for (i in 1..nMessages) {
                messageBus.send(queue, simpleEvent)
            }
            latch.await(20, TimeUnit.SECONDS)
            logger.info("done")
        }
    }

    @Test
    fun eventBusResendsEventAfterFailureInHandler() {
        val done = CompletableFuture<Event>()
        val failure = CompletableFuture<Event>()
        var eventCount = 0
        messageBus.receive(topic) {
            if (eventCount == 0) {
                println("received event, return failure")
                failure.complete(it)
                eventCount++
                Try.raise(IllegalStateException("could not handle event"))
            } else {
                println("received event, return complete")
                done.complete(it)
                Try.just(Unit)
            }
        }
        runAfterDelay(1, TimeUnit.SECONDS) {
            messageBus.send(topic, simpleEvent)
            val failedEvent = failure.get()
            val receivedEvent = done.get()
            logger.info("done, failedEvent=$failedEvent")
            logger.info("done, receivedEvent=$receivedEvent")
        }
    }

}