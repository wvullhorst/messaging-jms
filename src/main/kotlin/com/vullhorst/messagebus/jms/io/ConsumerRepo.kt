package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.retryOnce
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.model.consumerName
import mu.KotlinLogging
import javax.jms.MessageConsumer
import javax.jms.Topic

private val logger = KotlinLogging.logger {}

fun withConsumer(context: DestinationContext,
                 body: (MessageConsumer) -> Try<Unit>): Try<Unit> =
        createConsumer(context)
                .flatMap { consumer ->
                    retryOnce(context.shutDownSignal) {
                        body.invoke(consumer)
                    }
                }

private fun createConsumer(context: DestinationContext): Try<MessageConsumer> {
    return Try {
        logger.debug { "create new consumer for ${context.channel}" }
        when (context.destination) {
            is Topic -> context.session.createDurableSubscriber(context.destination, context.channel.consumerName())
            else -> context.session.createConsumer(context.destination)
        }
    }
}
