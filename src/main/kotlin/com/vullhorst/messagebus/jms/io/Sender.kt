package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import mu.KotlinLogging
import javax.jms.Message

private val logger = KotlinLogging.logger {}

fun <T> send(context: DestinationContext,
             serializer: (T) -> Try<Message>,
             anObject: T): Try<Unit> =
        serializer.invoke(anObject)
                .flatMap { message ->
                    withProducer(context) {
                        Try {
                            it.send(message)
                            logger.debug { "message sent successfully" }
                        }
                    }
                }
