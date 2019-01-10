package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.execution.handleIncomingMessages
import com.vullhorst.messagebus.jms.io.getSession
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.io.send
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import java.util.concurrent.Executors
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session

class Dispatcher(
        private val connectionBuilder: () -> Try<Connection>,
        private val receiveChannel: Channel,
        private val sendChannel: Channel,
        private val messageBuilder: (Session, Message) -> Try<Message>) {

    private val receiver = Executors.newSingleThreadExecutor()
    private val logger = KotlinLogging.logger {}

    fun startup() {
        receiver.execute {
            Thread.currentThread().name = "Dispatcher_rcv"
            logger.info { "startup" }
            getSession(connectionBuilder)
                    .combineWith { session -> session.createDestination(receiveChannel) }
                    .flatMap { Try { DestinationContext(it.first, it.second, receiveChannel) } }
                    .flatMap { destinationContext ->
                        handleIncomingMessages(destinationContext, { Try { it } }) {
                            send(it)
                        }
                    }
        }
    }

    private fun send(incomingMessage: Message): Try<Unit> {
        logger.info { "send..." }
        return getSession(connectionBuilder)
                .combineWith { session -> session.createDestination(sendChannel) }
                .flatMap { Try { DestinationContext(it.first, it.second, sendChannel) } }
                .flatMap { context ->
                    messageBuilder.invoke(context.session, incomingMessage).flatMap { message ->
                        send(context, message)
                    }
                }
    }

    fun shutdown() {
        logger.warn("shutting down...")
        receiver.shutdown()
        while (!receiver.isTerminated) {
        }
        logger.warn("shutdown completed")

    }
}
