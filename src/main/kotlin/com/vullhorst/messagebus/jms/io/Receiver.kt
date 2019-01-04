package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.retryOnFailure
import mu.KotlinLogging
import javax.jms.Destination
import javax.jms.Message
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun <T> handleTypedMessages(session: Session,
                            destination: Destination,
                            deserializer: (Message) -> Try<T>,
                            messageHandlerName: String,
                            shutdownSignal: () -> Boolean,
                            body: (T) -> Try<Unit>) =
        retryOnFailure {
            withConsumer(session, destination, messageHandlerName) { consumer ->
                Try {
                    while (!shutdownSignal.invoke()) {
                        withMessage(consumer, shutdownSignal) { message ->
                            convertToT(message, deserializer)
                                    .flatMap { messageTPair ->
                                        body.invoke(messageTPair.second)
                                                .map { messageTPair.first.acknowledge() }
                                                .recoverWith { exception ->
                                                    { throwable: Throwable ->
                                                        Try {
                                                            logger.warn { "error in message handling, recover" }
                                                            session.recover()
                                                            throw throwable
                                                        }
                                                    }.invoke(exception)
                                                }
                                    }
                        }
                    }
                }
            }
        }

private fun <T> convertToT(msg: Message,
                           deserializer: (Message) -> Try<T>): Try<Pair<Message, T>> =
        deserializer.invoke(msg).map { objectOfT -> Pair(msg, objectOfT) }
