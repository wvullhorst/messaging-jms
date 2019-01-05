package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.retry
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import mu.KotlinLogging
import javax.jms.Message

private val logger = KotlinLogging.logger {}

fun <T> withIncomingMessage(context: DestinationContext,
                            deserializer: (Message) -> Try<T>,
                            body: (T) -> Try<Unit>) =
        retry {
            withConsumer(context) { consumer ->
                Try {
                    context.shutDownSignal.repeatUntilShutDown {
                        withMessage(consumer, context.shutDownSignal) { message ->
                            convertToT(message, deserializer)
                                    .flatMap { messageTPair ->
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
        }

private fun <T> convertToT(msg: Message,
                           deserializer: (Message) -> Try<T>): Try<Pair<Message, T>> =
        deserializer.invoke(msg).map { objectOfT -> Pair(msg, objectOfT) }
