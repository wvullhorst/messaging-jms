package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.retryOnFailure
import mu.KotlinLogging
import javax.jms.Destination
import javax.jms.Message
import javax.jms.Session

private val logger = KotlinLogging.logger {}

fun handlePlainMessages(session: Session,
                        destination: Destination,
                        messageHandlerName: String,
                        shutdownSignal: () -> Boolean,
                        body: (Message) -> Try<Unit>) =
        retryOnFailure {
            withConsumer(session, destination, messageHandlerName) { consumer ->
                Try {
                    while (!shutdownSignal.invoke()) {
                        withMessage(consumer, shutdownSignal) { message ->
                            body.invoke(message)
                                    .map { message.acknowledge() }
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
