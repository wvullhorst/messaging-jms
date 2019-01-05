package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.SessionCache
import com.vullhorst.messagebus.jms.io.send
import com.vullhorst.messagebus.jms.io.withIncomingMessage
import com.vullhorst.messagebus.jms.model.Channel
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import mu.KotlinLogging
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session

class Dispatcher(
        connectionBuilder: () -> Connection,
        private val receiveChannel: Channel,
        private val sendChannel: Channel,
        private val messageBuilder: (Session, Message) -> Try<Message>) {

    private val sessionCache = SessionCache(connectionBuilder)
    private val logger = KotlinLogging.logger {}

    fun startup() {
        GlobalScope.launch(CoroutineName("Dispatcher-rcv")) {
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
        this.sessionCache.shutDown()
    }
}
