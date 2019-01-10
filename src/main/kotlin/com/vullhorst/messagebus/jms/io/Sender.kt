package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun <T> send(channel: Channel,
         objectOfT: T,
         connectionBuilder: () -> Try<Connection>,
         serializer: (Session, T) -> Try<Message>): Try<Unit> {
    logger.debug("send $channel")
    return getSession(connectionBuilder)
            .combineWith { session -> session.createDestination(channel) }
            .flatMap { Try { DestinationContext(it.first, it.second, channel) } }
            .flatMap { context ->
                serializer.invoke(context.session, objectOfT)
                        .flatMap { serialized -> send(context, serialized) }
            }
}

fun send(context: DestinationContext,
         message: Message): Try<Unit> =
        withProducer(context) {
            Try {
                it.send(message)
                logger.debug { "message sent successfully" }
            }
        }
