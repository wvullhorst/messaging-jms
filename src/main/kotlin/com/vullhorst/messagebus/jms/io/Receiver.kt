package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.execution.loopUntilShutdown
import com.vullhorst.messagebus.jms.execution.retryForever
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.io.model.buildDestinationContext
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.consumerName
import mu.KotlinLogging
import javax.jms.*

private val logger = KotlinLogging.logger {}

fun <T> receive(channel: Channel,
                deserializer: (Message) -> Try<T>,
                consumer: (T) -> Try<Unit>,
                sessionProvider: () -> Try<Session>,
                sessionInvalidator: () -> Try<Unit>,
                executor: (() -> Unit) -> Unit,
                shutDownSignal: () -> Boolean) =
        executor.invoke {
            logger.debug("starting receiver for channel $channel -> ${Thread.currentThread()}")
            loopUntilShutdown(shutDownSignal) {
                sessionProvider.invoke()
                        .andThen { buildDestinationContext(channel, sessionProvider) }
                        .andThen { context ->
                            handleIncomingMessages(context, deserializer, shutDownSignal, consumer)
                                    .recoverWith { sessionInvalidator.invoke() }
                        }
            }
        }

private fun <T> handleIncomingMessages(context: DestinationContext,
                                       deserializer: (Message) -> Try<T>,
                                       shutDownSignal: () -> Boolean,
                                       body: (T) -> Try<Unit>): Try<Unit> =
        retryForever(shutDownSignal = shutDownSignal) {
            createConsumer(context)
                    .andThen { consumer ->
                        loopUntilShutdown(shutDownSignal) {
                            receiveMessage(consumer, shutDownSignal)
                                    .combineWith { deserializer.invoke(it) }
                                    .andThen { callConsumerAndHandleResult(body, it, context) }
                        }
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

private fun receiveMessage(consumer: MessageConsumer,
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

private fun <T> callConsumerAndHandleResult(body: (T) -> Try<Unit>, messageTPair: Pair<Message, T>, context: DestinationContext): Try<Unit> {
    return body.invoke(messageTPair.second)
            .map { messageTPair.first.acknowledge() }
            .recoverWith { exception ->
                { throwable: Throwable ->
                    Try {
                        logger.warn { "error in message handling, recover" }
                        context.session.recover()
                        throw throwable
                    }
                }.invoke(exception)
            }
}
