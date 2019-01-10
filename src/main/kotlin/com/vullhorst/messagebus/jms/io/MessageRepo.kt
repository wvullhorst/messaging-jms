package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import mu.KotlinLogging
import javax.jms.Message
import javax.jms.MessageConsumer

private val logger = KotlinLogging.logger {}

fun receiveMessage(consumer: MessageConsumer,
                   stopSignal: () -> Boolean): Try<Message> {
    logger.debug("receiveLoop")
    return Try {
        var message: Message? = null
        while (!stopSignal.invoke() && message == null) {
            message = consumer.receive(1000)
        }
        logger.info("-> $message")
        message!!
    }
}
