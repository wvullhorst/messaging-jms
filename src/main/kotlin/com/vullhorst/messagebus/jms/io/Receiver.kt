package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.execution.loopUntilShutdown
import com.vullhorst.messagebus.jms.execution.retryForever
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import javax.jms.Connection
import javax.jms.Destination
import javax.jms.Message
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun <T> receive(channel: Channel,
                numberOfConsumers: Int = 1,
                sessionHolder: SessionHolder,
                connectionBuilder: () -> Try<Connection>,
                executor: (() -> Unit) -> Unit,
                deserializer: (Message) -> Try<T>,
                shutDownSignal: () -> Boolean,
                consumer: (T) -> Try<Unit>) {
    (1..numberOfConsumers).forEach { consumerId ->
        executor.invoke {
            Thread.currentThread().name = "MessageBus_rcv-$consumerId"
            logger.debug("$consumerId starting receiver for channel $channel -> ${Thread.currentThread()}")
            loopUntilShutdown(shutDownSignal) {
                getSession(sessionHolder, connectionBuilder)
                        .combineWith { session -> session.createDestination(channel) }
                        .andThen { buildDestinationContext(it, channel) }
                        .andThen { destinationContext ->
                            handleIncomingMessages(destinationContext, deserializer, shutDownSignal, consumer)
                        }
                        .recoverWith { Try { invalidateSession(sessionHolder) } }
            }
        }
    }
}

private fun buildDestinationContext(it: Pair<Session, Destination>, channel: Channel) = Try { DestinationContext(it.first, it.second, channel) }

private fun <T> handleIncomingMessages(context: DestinationContext,
                                       deserializer: (Message) -> Try<T>,
                                       shutDownSignal: () -> Boolean,
                                       body: (T) -> Try<Unit>): Try<Unit> =
        retryForever(shutDownSignal = shutDownSignal) {
            createConsumer(context)
                    .andThen { consumer ->
                        loopUntilShutdown(shutDownSignal) {
                            receiveMessage(consumer, shutDownSignal)
                                    .andThen {
                                        convertToT(it, deserializer)
                                                .andThen { messageTPair ->
                                                    body.invoke(messageTPair.second)
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
                                    }
                        }
                    }
        }

private fun <T> convertToT(msg: Message,
                           deserializer: (Message) -> Try<T>): Try<Pair<Message, T>> =
        deserializer.invoke(msg).map { objectOfT -> Pair(msg, objectOfT) }
