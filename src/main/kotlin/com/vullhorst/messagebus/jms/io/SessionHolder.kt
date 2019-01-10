package com.vullhorst.messagebus.jms.io

import arrow.core.Option
import arrow.core.Try
import arrow.core.getOrElse
import mu.KotlinLogging
import java.util.concurrent.locks.ReentrantLock
import javax.jms.Connection
import javax.jms.Session

fun getSession(connectionBuilder: () -> Try<Connection>): Try<Session> =
        getOrCreateSession(connectionBuilder,
                { SessionHolder.sessionContext },
                { SessionHolder.sessionContext = it })

private val logger = KotlinLogging.logger {}

private data class SessionContext(
        val connection: Connection,
        val session: Session
)

private object SessionHolder {
    var sessionContext: Option<SessionContext> = Option.empty()
}

private val sessionLock = ReentrantLock()
private fun getOrCreateSession(connectionBuilder: () -> Try<Connection>,
                               contextGetter: () -> Option<SessionContext>,
                               contextSetter: (Option<SessionContext>) -> Unit): Try<Session> {
    try {
        sessionLock.lock()
        return contextGetter.invoke().map {
            logger.debug("using existing session")
            Try.just(it.session)
        }.getOrElse {
            contextSetter.invoke(buildSessionContext(connectionBuilder).toOption())
            contextGetter.invoke()
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

fun invalidateSession(): () -> Unit = {
    logger.info { "invalidating session cache" }
    SessionHolder.sessionContext = Option.empty()
}
