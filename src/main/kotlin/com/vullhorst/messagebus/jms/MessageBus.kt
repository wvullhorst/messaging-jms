package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.receive
import com.vullhorst.messagebus.jms.io.send
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

    fun send(channel: Channel, objectOfT: T): Try<Unit> {
        logger.debug("send $channel")
        return send(channel,
                objectOfT,
                connectionBuilder,
                serializer)
    }

    fun receive(channel: Channel,
                numberOfConsumers: Int = 1,
                consumer: (T) -> Try<Unit>) {
        receive(channel,
                numberOfConsumers,
                connectionBuilder,
                { receivers.execute(it) },
                deserializer,
                { shutDownSignal },
                consumer)
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
