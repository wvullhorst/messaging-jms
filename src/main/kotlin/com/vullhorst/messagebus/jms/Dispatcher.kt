package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.SessionHolder
import com.vullhorst.messagebus.jms.io.receive
import com.vullhorst.messagebus.jms.io.send
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

    private val receiver = Executors.newSingleThreadExecutor()
    private var shutDownSignal: Boolean = false
    private val sessionHolder = SessionHolder()

    fun startup() {
        receiver.execute {
            Thread.currentThread().name = "Dispatcher_rcv"
            logger.info { "startup" }
            receive(receiveChannel,
                    1,
                    sessionHolder,
                    connectionBuilder,
                    { receiver.execute(it) },
                    { msg -> Try { msg } },
                    { shutDownSignal },
                    { send(it) })
        }
    }

    private fun send(incomingMessage: Message): Try<Unit> {
        logger.info { "send..." }
        return send(sendChannel,
                incomingMessage,
                sessionHolder,
                connectionBuilder,
                messageBuilder)
    }

    fun shutdown() {
        logger.warn("shutting down...")
        shutDownSignal = true
        receiver.shutdown()
        while (!receiver.isTerminated) {
        }
        logger.warn("shutdown completed")

    }
}
