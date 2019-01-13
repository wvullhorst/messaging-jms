package com.vullhorst.messagebus.jms

import arrow.core.Try
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
        receivers.execute {
            Thread.currentThread().name = "Dispatcher_rcv"
            handleIncomingMessages(receiveChannel,
                    { msg -> Try { msg } },
                    { send(it) },
                    { getSession(sessionHolder, connectionBuilder) },
                    { invalidateSession(sessionHolder) },
                    { shutDownSignal })
        }
    }

    private fun send(incomingMessage: Message): Try<Unit> {
        logger.info { "send..." }
        return sendTo(sendChannel,
                incomingMessage,
                messageBuilder,
                { getSession(sessionHolder, connectionBuilder) },
                { invalidateSession(sessionHolder) },
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
