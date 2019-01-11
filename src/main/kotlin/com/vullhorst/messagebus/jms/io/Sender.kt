package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.io.model.buildDestinationContext
import com.vullhorst.messagebus.jms.model.Channel
import mu.KotlinLogging
import javax.jms.Connection
import javax.jms.Message
import javax.jms.MessageProducer
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun <T> send(channel: Channel,
             objectOfT: T,
             sessionHolder: SessionHolder,
             connectionBuilder: () -> Try<Connection>,
             serializer: (Session, T) -> Try<Message>): Try<Unit> {
    logger.debug("send $channel")
    return buildDestinationContext(channel, sessionHolder, connectionBuilder)
            .combineWith { serializer.invoke(it.session, objectOfT) }
            .andThen { send(it.first, it.second) }
}

private fun send(context: DestinationContext,
                 message: Message): Try<Unit> =
        Try { context.session.createProducer(context.destination) }
                .andThen { sendMessage(it, message) }

private fun sendMessage(producer: MessageProducer,
                        message: Message) = Try {
    producer.send(message)
    logger.debug { "message sent successfully" }
}