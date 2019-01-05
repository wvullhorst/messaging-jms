package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.SessionCache
import com.vullhorst.messagebus.jms.io.handlePlainMessages
import com.vullhorst.messagebus.jms.io.send
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.consumerNmae
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

    private var shutdownSignal = false

    fun startup() {
        this.shutdownSignal = false
        GlobalScope.launch(CoroutineName("Dispatcher-rcv")) {
            logger.info { "startup" }
            sessionCache.onSession(receiveChannel) { session, destination ->
                handlePlainMessages(session,
                        destination,
                        receiveChannel.consumerNmae(),
                        { shutdownSignal }) { incomingMessage ->
                    send(incomingMessage)
                }
            }
        }
    }

    private fun send(incomingMessage: Message): Try<Unit> {
        logger.info { "send..." }
        return sessionCache.onSession(sendChannel) { session, destination ->
            messageBuilder.invoke(session, incomingMessage).flatMap { outgoingMessage ->
                send(session, destination, outgoingMessage)
            }
        }
    }

    fun shutdown() {
        this.shutdownSignal = true
    }
}
