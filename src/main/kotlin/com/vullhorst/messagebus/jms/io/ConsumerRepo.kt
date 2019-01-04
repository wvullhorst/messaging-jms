package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.retryOnce
import mu.KotlinLogging
import javax.jms.Destination
import javax.jms.MessageConsumer
import javax.jms.Session
import javax.jms.Topic

private val logger = KotlinLogging.logger {}

fun withConsumer(session: Session,
                 destination: Destination,
                 messageHandlerName: String,
                 body: (MessageConsumer) -> Try<Unit>): Try<Unit> =
        createConsumer(session, destination, messageHandlerName)
                .flatMap { consumer ->
                    retryOnce {
                        body.invoke(consumer)
                    }
                }

private fun createConsumer(session: Session,
                           destination: Destination,
                           messageHandlerName: String): Try<MessageConsumer> {
    return Try {
        logger.debug { "create new consumer for $destination" }
        when (destination) {
            is Topic -> session.createDurableSubscriber(destination, messageHandlerName)
            else -> session.createConsumer(destination)
        }
    }
}
