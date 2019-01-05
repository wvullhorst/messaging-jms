package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import mu.KotlinLogging
import javax.jms.Message
import javax.jms.MessageProducer

private val logger = KotlinLogging.logger {}

fun <T> send(context: DestinationContext,
             serializer: (T) -> Try<Message>,
             anObject: T): Try<Unit> =
        serializer.invoke(anObject)
                .flatMap { message ->
                    send(context.session.createProducer(context.destination), message)
                }

private fun send(producer: MessageProducer,
                 message: Message): Try<Unit> =
        Try {
            producer.send(message)
            logger.debug { "message sent successfully" }
        }
