package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.execute
import com.vullhorst.messagebus.jms.io.*
import com.vullhorst.messagebus.jms.model.Channel
import mu.KotlinLogging
import java.util.concurrent.Executors
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session

private val logger = KotlinLogging.logger {}

class MessageBus<T>(
        private val connectionBuilder: () -> Try<Connection>,
        private val serializer: (Session, T) -> Try<Message>,
        private val deserializer: (Message) -> Try<T>) {

    private val receivers = Executors.newCachedThreadPool()
    private var shutDownSignal: Boolean = false
    private val sessionHolder = SessionHolder()

    fun send(channel: Channel, objectOfT: T): Try<Unit> {
        logger.debug("send $channel")
        return send(channel,
                objectOfT,
                { getSession(sessionHolder, connectionBuilder) },
                serializer)
    }

    fun receive(channel: Channel,
                numberOfConsumers: Int = 1,
                consumer: (T) -> Try<Unit>) {
        receive(channel,
                deserializer,
                consumer,
                { getSession(sessionHolder, connectionBuilder) },
                { invalidateSession(sessionHolder) },
                { receivers.execute(numberOfConsumers, { id -> "MessageBus_rcv$id" }, it) },
                { shutDownSignal })
    }

    fun shutdown() {
        logger.warn("shutting down...")
        shutDownSignal = true
        receivers.shutdown()
        while (!receivers.isTerminated) {
        }
        logger.warn("shutdown completed")
    }
}
