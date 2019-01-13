package com.vullhorst.messagebus.jms.io

import arrow.core.Option
import arrow.core.Try
import arrow.core.getOrElse
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.execution.combineWith
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
        getOrCreateSession(sessionHolder, connectionBuilder)

fun invalidateSession(sessionHolder: SessionHolder): Try<Unit> = Try {
    logger.info { "invalidating session cache" }
    sessionHolder.sessionContext.map {
        logger.info("closing session and connection")
        it.session.close()
        it.connection.stop()
        it.connection.close()
        Try.just(Unit)
    }
    sessionHolder.sessionContext = Option.empty()
}

private val sessionLock = ReentrantLock()
private fun getOrCreateSession(sessionHolder: SessionHolder,
                               connectionBuilder: () -> Try<Connection>): Try<Session> {
    try {
        sessionLock.lock()
        return sessionHolder.sessionContext
                .map {
                    logger.debug("using existing session")
                    Try.just(it.session)
                }
                .getOrElse {
                    buildAndStoreContext(sessionHolder, connectionBuilder)
                }
    } finally {
        sessionLock.unlock()
    }
}

fun buildAndStoreContext(sessionHolder: SessionHolder,
                         connectionBuilder: () -> Try<Connection>) =
        buildSessionContext(sessionHolder, connectionBuilder)
                .map {
                    sessionHolder.sessionContext = Option.just(it)
                    it.session
                }

private fun buildSessionContext(sessionHolder: SessionHolder,
                                connectionBuilder: () -> Try<Connection>): Try<SessionContext> {
    logger.info("creating new connection and session")
    return connectionBuilder.invoke()
            .andThen { connection -> startConnection(sessionHolder, connection) }
            .combineWith { connection -> createSession(connection) }
            .map { SessionContext(it.first, it.second) }
}


private fun startConnection(sessionHolder: SessionHolder,
                            connection: Connection): Try<Connection> {
    return Try {
        // connection.setExceptionListener { invalidateSession(sessionHolder) }
        connection.start()
        connection
    }
}

private fun createSession(connection: Connection): Try<Session> =
        Try { connection.createSession(false, Session.CLIENT_ACKNOWLEDGE) }
