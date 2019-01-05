package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.SessionCache
import com.vullhorst.messagebus.jms.io.handleTypedMessages
import com.vullhorst.messagebus.jms.io.send
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.consumerNmae
import mu.KotlinLogging
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

    private var shutdownSignal = false

    fun startup() {
        logger.warn("starting up")
        this.shutdownSignal = false
    }

    fun send(channel: Channel, objectOfT: T): Try<Unit> {
        logger.debug("send $channel");
        return sessionCache.onSession(channel) { session, destination ->
            send(session,
                    destination,
                    { obj -> this.serializer.invoke(session, obj) },
                    objectOfT)
        }
    }

    fun receive(channel: Channel,
                numberOfConsumers: Int = 1,
                consumer: (T) -> Try<Unit>) {
        (1..numberOfConsumers).forEach { consumerId ->
            thread(name = "MessageBus-rcv_$consumerId") {
                logger.debug("$consumerId starting receiver for channel $channel -> ${Thread.currentThread()}")
                sessionCache.onSession(channel) { session, destination ->
                    handleTypedMessages(session,
                            destination,
                            deserializer,
                            channel.consumerNmae(),
                            { this.shutdownSignal },
                            consumer)
                }
            }
        }
    }

    fun shutdown() {
        logger.warn("shutting down")
        this.shutdownSignal = true
        sessionCache.disconnect()
    }
}
