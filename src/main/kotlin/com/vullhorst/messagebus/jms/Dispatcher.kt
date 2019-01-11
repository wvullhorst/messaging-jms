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

class Dispatcher(
        private val connectionBuilder: () -> Try<Connection>,
        private val receiveChannel: Channel,
        private val sendChannel: Channel,
        private val messageBuilder: (Session, Message) -> Try<Message>) {

    private val receivers = Executors.newSingleThreadExecutor()
    private var shutDownSignal: Boolean = false
    private val sessionHolder = SessionHolder()

    fun startup() {
        logger.info { "startup" }
        receive(receiveChannel,
                { msg -> Try { msg } },
                { send(it) },
                { getSession(sessionHolder, connectionBuilder) },
                { invalidateSession(sessionHolder) },
                { receivers.execute(1, { id -> "Dispatcher_rcv$id" }, it) },
                { shutDownSignal })
    }

    private fun send(incomingMessage: Message): Try<Unit> {
        logger.info { "send..." }
        return send(sendChannel,
                incomingMessage,
                { getSession(sessionHolder, connectionBuilder) },
                messageBuilder)
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
