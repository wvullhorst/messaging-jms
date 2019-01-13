package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.*
import com.vullhorst.messagebus.jms.model.Channel
import mu.KotlinLogging
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session
import kotlin.concurrent.thread

private val logger = KotlinLogging.logger {}

class MessageBus<T>(
        private val connectionBuilder: () -> Try<Connection>,
        private val serializer: (Session, T) -> Try<Message>,
        private val deserializer: (Message) -> Try<T>) {

    private val sessionHolder = SessionHolder()

    private var shutDownSignal: Boolean = false

    fun send(channel: Channel, objectOfT: T): Try<Unit> {
        logger.debug("send $channel")
        return sendTo(channel,
                objectOfT,
                serializer,
                { getSession(sessionHolder, connectionBuilder) },
                { invalidateSession(sessionHolder) },
                { shutDownSignal })
    }

    fun receive(channel: Channel,
                numberOfConsumers: Int = 1,
                consumer: (T) -> Try<Unit>) {
        (1..numberOfConsumers).forEach {
            thread {
                Thread.currentThread().name = "MessageBus_rcv$it"
                readNextMessage(channel,
                        deserializer,
                        consumer,
                        { getSession(sessionHolder, connectionBuilder) },
                        { invalidateSession(sessionHolder) },
                        { shutDownSignal })
            }
        }
    }

    fun shutdown() {
        logger.warn("shutting down...")
        shutDownSignal = true
        logger.warn("shutdown completed")
    }
}
