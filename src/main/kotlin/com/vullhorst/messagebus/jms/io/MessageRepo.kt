package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import mu.KotlinLogging
import javax.jms.Message
import javax.jms.MessageConsumer

private val logger = KotlinLogging.logger {}

fun receiveMessage(consumer: MessageConsumer,
                   shutDownSignal: () -> Boolean): Try<Message> {
    return Try {
        var message: Message? = null
        while (!shutDownSignal.invoke() && message == null) {
            message = consumer.receive(1000)
        }
        logger.info("-> $message")
        message!!
    }
}
