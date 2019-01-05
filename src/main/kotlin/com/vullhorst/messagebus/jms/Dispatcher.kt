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

class Dispatcher(
        connectionBuilder: () -> Connection,
        private val receiveChannel: Channel,
        private val sendChannel: Channel,
        private val messageBuilder: (Session, Message) -> Try<Message>) {

    private val sessionCache = SessionCache(connectionBuilder)
    private val receiver = Executors.newSingleThreadExecutor()
    private val logger = KotlinLogging.logger {}

    fun startup() {
        receiver.execute {
            Thread.currentThread().name = "Dispatcher_rcv"
            logger.info { "startup" }
            sessionCache.onSession(receiveChannel) { ctx ->
                withIncomingMessage(ctx,
                        { Try.just(it) }) {
                    send(it)
                }
            }

        }
    }

    private fun send(incomingMessage: Message): Try<Unit> {
        logger.info { "send..." }
        return sessionCache.onSession(sendChannel) { ctx ->
            messageBuilder.invoke(ctx.session, incomingMessage).flatMap { outgoingMessage ->
                send(ctx,
                        { Try.just(it) },
                        outgoingMessage)
            }
        }
    }

    fun shutdown() {
        logger.warn("shutting down...")
        this.sessionCache.shutDown()
        receiver.shutdown()
        while (!receiver.isTerminated) { }
        logger.warn("shutdown completed")

    }
}
