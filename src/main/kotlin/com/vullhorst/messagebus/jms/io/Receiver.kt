package com.vullhorst.messagebus.jms.io

import arrow.core.Either
import arrow.core.Try
import arrow.core.orElse
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.*
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.consumerName
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import javax.jms.Message
import javax.jms.MessageConsumer
import javax.jms.Session
import javax.jms.Topic

private val logger = KotlinLogging.logger {}

fun <T> handleIncomingMessages(channel: Channel,
                               deserializer: (Message) -> Try<T>,
                               consumer: (T) -> Try<Unit>,
                               sessionProvider: () -> Try<Session>,
                               sessionInvalidator: () -> Try<Unit>,
                               shutDownSignal: () -> Boolean): Try<Unit> {
    logger.debug("starting receiver for channel $channel -> ${Thread.currentThread()}")
    return loopUntilShutdown(shutDownSignal) {
        sessionProvider.invoke().flatMap { session ->
            session.createDestination(channel)
                    .andThen { destination ->
                        handleIncomingMessages(DestinationContext(session, destination, channel),
                                deserializer,
                                shutDownSignal,
                                consumer)
                                .recoverWith { error ->
                                    logger.error("error handling message: ${error.message}")
                                    sessionInvalidator.invoke()
                                }
                    }
        }
    }
}


private fun <T> handleIncomingMessages(context: DestinationContext,
                                       deserializer: (Message) -> Try<T>,
                                       shutDownSignal: () -> Boolean,
                                       body: (T) -> Try<Unit>): Try<Unit> =
        closeAfterUsage("consumer",
                { createConsumer(context) },
                { Try { it.close() } }) { consumer ->
            loopUntilFails(shutDownSignal = shutDownSignal) {
                handleIncomingMessages(consumer, shutDownSignal)
                        .andThen { messageOrNot ->
                            messageOrNot.fold(
                                    { consumeMessage(it, deserializer, context.session, body) },
                                    { Try.just(Unit) })
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

private fun handleIncomingMessages(consumer: MessageConsumer,
                                   shutDownSignal: () -> Boolean): Try<Either<Message, Unit>> {
    while (true) {
        if (shutDownSignal.invoke()) {
            logger.debug("shutdown signal received, stop reading messages")
            return Try.just(Either.right(Unit))
        }
        Try.invoke {
            val message = consumer.receive(1000)
            if (message != null) return Try.just(Either.left(message))
        }
    }
}

private fun <T> consumeMessage(message: Message,
                               deserializer: (Message) -> Try<T>,
                               session: Session,
                               body: (T) -> Try<Unit>): Try<Unit> {
    return deserializer.invoke(message)
            .andThen { objectOfT ->
                body.invoke(objectOfT)
                        .map { message.acknowledge() }
                        .orElse { Try { session.recover() } }
            }
}