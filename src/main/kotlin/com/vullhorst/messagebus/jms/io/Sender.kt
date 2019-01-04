package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import mu.KotlinLogging
import javax.jms.Destination
import javax.jms.Message
import javax.jms.MessageProducer
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun <T> send(session: Session,
             destination: Destination,
             serializer: (T) -> Try<Message>,
             anObject: T): Try<Unit> =
        serializer.invoke(anObject)
                .flatMap { message ->
                    send(session.createProducer(destination), message)
                }

fun send(session: Session,
         destination: Destination,
         message: Message): Try<Unit> =
        send(session.createProducer(destination), message)

private fun send(producer: MessageProducer,
                 message: Message): Try<Unit> =
        Try {
            producer.send(message)
            logger.debug { "message sent successfully" }
        }
