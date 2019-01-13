package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.execution.closeAfterUsage
import com.vullhorst.messagebus.jms.execution.invalidateOnFailure
import com.vullhorst.messagebus.jms.execution.retryForever
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import javax.jms.Destination
import javax.jms.Message
import javax.jms.MessageProducer
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun <T> sendTo(channel: Channel,
               objectOfT: T,
               serializer: (Session, T) -> Try<Message>,
               sessionProvider: () -> Try<Session>,
               sessionInvalidator: () -> Try<Unit>,
               shutDownSignal: () -> Boolean): Try<Unit> =
        retryForever(shutDownSignal = shutDownSignal) {
            invalidateOnFailure("session", sessionProvider, sessionInvalidator) { session ->
                session.createDestination(channel)
                        .andThen { destination ->
                            serializer.invoke(session, objectOfT)
                                    .andThen { send(session, destination, it) }
                        }
            }
        }


private fun send(session: Session,
                 destination: Destination,
                 message: Message): Try<Unit> =
        closeAfterUsage("producer",
                { Try { session.createProducer(destination) } },
                { Try { it.close() } }) {
            sendMessage(it, message)
        }

private fun sendMessage(producer: MessageProducer,
                        message: Message) = Try {
    producer.send(message)
    logger.debug { "message sent successfully" }
}