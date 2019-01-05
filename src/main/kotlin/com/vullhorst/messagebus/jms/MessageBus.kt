package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.SessionCache
import com.vullhorst.messagebus.jms.io.send
import com.vullhorst.messagebus.jms.io.withIncomingMessage
import com.vullhorst.messagebus.jms.model.Channel
import mu.KotlinLogging
import java.util.concurrent.Executors
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session
import kotlin.concurrent.thread

class MessageBus<T>(
        connectionBuilder: () -> Connection,
        private val serializer: (Session, T) -> Try<Message>,
        private val deserializer: (Message) -> Try<T>) {

    private val logger = KotlinLogging.logger {}

    private val sessionCache = SessionCache(connectionBuilder)
    private val receivers = Executors.newCachedThreadPool()

    fun send(channel: Channel, objectOfT: T): Try<Unit> {
        logger.debug("send $channel")
        return sessionCache.onSession(channel) { context ->
            send(context,
                    { obj -> this.serializer.invoke(context.session, obj) },
                    objectOfT)
        }
    }

    fun receive(channel: Channel,
                numberOfConsumers: Int = 1,
                consumer: (T) -> Try<Unit>) {
        (1..numberOfConsumers).forEach { consumerId ->
            receivers.execute {
                Thread.currentThread().name = "MessageBus_rcv-$consumerId"
                logger.debug("$consumerId starting receiver for channel $channel -> ${Thread.currentThread()}")
                sessionCache.onSession(channel) { context ->
                    withIncomingMessage(context,
                            deserializer,
                            consumer)
                }
            }
        }
    }

    fun shutdown() {
        logger.warn("shutting down")
        sessionCache.shutDown()
        receivers.shutdown()
        while(!receivers.isTerminated) {}
    }
}
