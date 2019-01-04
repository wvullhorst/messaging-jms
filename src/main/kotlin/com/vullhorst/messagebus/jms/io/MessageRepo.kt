package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import mu.KotlinLogging
import javax.jms.Message
import javax.jms.MessageConsumer

private val logger = KotlinLogging.logger {}

fun withMessage(consumer: MessageConsumer,
                shutdownSignal: () -> Boolean,
                body: (Message) -> Try<Unit>): Try<Unit> {
    logger.debug("receiveLoop")
    return Try {
        var message: Message? = null
        while (message == null && !shutdownSignal.invoke()) {
            message = consumer.receive(1000)
        }
        logger.info("-> $message")
        message!!
    }.flatMap { message ->
        body.invoke(message)
    }
}
