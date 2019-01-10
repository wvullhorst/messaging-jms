package com.vullhorst.messagebus.jms

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.ShutdownSignal
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.execution.handleIncomingMessages
import com.vullhorst.messagebus.jms.execution.loopUntilShutdown
import com.vullhorst.messagebus.jms.io.getSession
import com.vullhorst.messagebus.jms.io.invalidateSession
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import com.vullhorst.messagebus.jms.io.send
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import java.util.concurrent.Executors
import javax.jms.Connection
import javax.jms.Message
import javax.jms.Session

class MessageBus<T>(
        private val connectionBuilder: () -> Try<Connection>,
        private val serializer: (Session, T) -> Try<Message>,
        private val deserializer: (Message) -> Try<T>) {

    private val logger = KotlinLogging.logger {}

    private val receivers = Executors.newCachedThreadPool()

    fun send(channel: Channel, objectOfT: T): Try<Unit> {
        logger.debug("send $channel")
        return getSession(connectionBuilder)
                .combineWith { session -> session.createDestination(channel) }
                .flatMap { Try { DestinationContext(it.first, it.second, channel) } }
                .flatMap { context ->
                    this.serializer.invoke(context.session, objectOfT)
                            .flatMap { serialized -> send(context, serialized) }
                }
    }

    fun receive(channel: Channel,
                numberOfConsumers: Int = 1,
                consumer: (T) -> Try<Unit>) {
        (1..numberOfConsumers).forEach { consumerId ->
            receivers.execute {
                Thread.currentThread().name = "MessageBus_rcv-$consumerId"
                logger.debug("$consumerId starting receiver for channel $channel -> ${Thread.currentThread()}")
                loopUntilShutdown {
                    getSession(connectionBuilder)
                            .combineWith { session -> session.createDestination(channel) }
                            .flatMap { Try { DestinationContext(it.first, it.second, channel) } }
                            .flatMap { destinationContext ->
                                handleIncomingMessages(destinationContext, deserializer, consumer)
                            }
                            .recoverWith { Try { invalidateSession() } }
                }
            }
        }
    }

    fun shutdown() {
        logger.warn("shutting down...")
        ShutdownSignal.set()
        receivers.shutdown()
        while (!receivers.isTerminated) {
        }
        logger.warn("shutdown completed")
    }
}
