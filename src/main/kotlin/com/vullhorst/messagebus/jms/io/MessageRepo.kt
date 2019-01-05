package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.model.ShutDownSignal
import mu.KotlinLogging
import javax.jms.Message
import javax.jms.MessageConsumer

private val logger = KotlinLogging.logger {}

fun withMessage(consumer: MessageConsumer,
                shutdownSignal: ShutDownSignal,
                body: (Message) -> Try<Unit>): Try<Unit> {
    logger.debug("receiveLoop")
    return Try {
        var message: Message? = null
        shutdownSignal.repeatUntilShutDown({message == null}) {
            message = consumer.receive(1000)
        }
        logger.info("-> $message")
        message!!
    }.flatMap { message ->
        body.invoke(message)
    }
}
