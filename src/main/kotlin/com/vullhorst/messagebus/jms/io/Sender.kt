package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import mu.KotlinLogging
import javax.jms.Message

private val logger = KotlinLogging.logger {}

fun send(context: DestinationContext,
         message: Message): Try<Unit> =
        withProducer(context) {
            Try {
                it.send(message)
                logger.debug { "message sent successfully" }
            }
        }
