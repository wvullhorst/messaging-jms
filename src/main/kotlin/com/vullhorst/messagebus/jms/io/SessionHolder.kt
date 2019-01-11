package com.vullhorst.messagebus.jms.io

import arrow.core.Option
import arrow.core.Try
import arrow.core.getOrElse
import mu.KotlinLogging
import java.util.concurrent.locks.ReentrantLock
import javax.jms.Connection
import javax.jms.Session

private val logger = KotlinLogging.logger {}

data class SessionContext(
        val connection: Connection,
        val session: Session
)

data class SessionHolder(
        var sessionContext: Option<SessionContext> = Option.empty()
)

fun getSession(sessionHolder: SessionHolder,
               connectionBuilder: () -> Try<Connection>): Try<Session> =
        getOrCreateSession(sessionHolder,
                connectionBuilder)

fun invalidateSession(sessionHolder: SessionHolder) = Try {
    logger.info { "invalidating session cache" }
    sessionHolder.sessionContext.exists {
        logger.info("closing session and connection")
        it.session.close()
        it.connection.close()
        true
    }
    sessionHolder.sessionContext = Option.empty()
}

private val sessionLock = ReentrantLock()
private fun getOrCreateSession(sessionHolder: SessionHolder,
                               connectionBuilder: () -> Try<Connection>): Try<Session> {
    try {
        sessionLock.lock()
        return sessionHolder.sessionContext.map {
            logger.debug("using existing session")
            Try.just(it.session)
        }.getOrElse {
            sessionHolder.sessionContext = buildSessionContext(connectionBuilder).toOption()
            sessionHolder.sessionContext
                    .map { Try.just(it.session) }
                    .getOrElse { Try.raise(IllegalStateException("could not create session")) }
        }
    } finally {
        sessionLock.unlock()
    }
}

private fun buildSessionContext(connectionBuilder: () -> Try<Connection>): Try<SessionContext> {
    logger.info("creating new connection and session")
    return connectionBuilder.invoke().map { connection ->
        connection.start()
        SessionContext(
                connection,
                connection.createSession(false, Session.CLIENT_ACKNOWLEDGE))
    }
}
