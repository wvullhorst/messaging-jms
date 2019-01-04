package com.vullhorst.messagebus.jms.io

import arrow.core.Option
import arrow.core.Try
import arrow.core.getOrElse
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.execution.retryForever
import com.vullhorst.messagebus.jms.execution.retryOnce
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import mu.KotlinLogging
import org.apache.activemq.ActiveMQConnectionFactory
import java.util.concurrent.locks.ReentrantLock
import javax.jms.Destination
import javax.jms.Session

class SessionCache(
        private val connectionFactoryProvider: () -> ActiveMQConnectionFactory,
        private val retryTimerInSeconds: Int = 10) {

    private var session: Option<Session> = Option.empty()

    private val logger = KotlinLogging.logger {}

    fun onSession(channel: Channel,
                  body: (Session, Destination) -> Try<Unit>): Try<Unit> =
            Try {
                retryForever(this.retryTimerInSeconds) {
                    session().flatMap { session ->
                        invoke(session, channel, body)
                                .recoverWith {
                                    logger.debug("$this: error on session: ${it.message}, invalidate session")
                                    invalidate()
                                    Try.raise(it)
                                }
                    }.recoverWith {
                        logger.debug { "$this: could not create session: ${it.message}, wait..." }
                        Try.raise(it)
                    }
                }
            }

    private fun invoke(session: Session,
                       channel: Channel,
                       body: (Session, Destination) -> Try<Unit>): Try<Unit> {
        return retryOnce {
            onDestination(session, channel) { destination ->
                body.invoke(session, destination)
            }
        }
    }

    private fun onDestination(session: Session,
                              channel: Channel,
                              body: (Destination) -> Try<Unit>): Try<Unit> =
            session.createDestination(channel)
                    .flatMap { destination -> body.invoke(destination) }

    private val sessionLock = ReentrantLock()
    private fun session(): Try<Session> {
        try {
            sessionLock.lock()
            return session.map {
                logger.debug("$this: using existing session")
                Try.just(it)
            }.getOrElse { build() }
        } finally {
            sessionLock.unlock()
        }
    }

    private fun build(): Try<Session> =
            Try {
                logger.info("creating new connection and session")
                val connection = connectionFactoryProvider
                        .invoke()
                        .createConnection()
                connection.start()
                connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
            }
                    .map {
                        logger.debug("storing new session...")
                        this.session = Option.just(it)
                        it
                    }

    fun invalidate(): Try<Unit> = Try {
        logger.info { "invalidating session cache" }
        this.session = Option.empty()
    }

    fun disconnect() {
        session.map {
            logger.info("closing session $it")
            it.close()
        }
        invalidate()
    }
}
