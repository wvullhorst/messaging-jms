package com.vullhorst.messagebus.jms.io.model

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.execution.combineWith
import com.vullhorst.messagebus.jms.io.SessionHolder
import com.vullhorst.messagebus.jms.io.getSession
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import javax.jms.Connection
import javax.jms.Destination
import javax.jms.Session

data class DestinationContext(val session: Session,
                              val destination: Destination,
                              val channel: Channel)

fun buildDestinationContext(channel: Channel,
                            sessionHolder: SessionHolder,
                            connectionBuilder: () -> Try<Connection>) =
        getSession(sessionHolder, connectionBuilder)
                .combineWith { session -> session.createDestination(channel) }
                .andThen { buildDestinationContext(it, channel) }

private fun buildDestinationContext(it: Pair<Session, Destination>, channel: Channel) = Try { DestinationContext(it.first, it.second, channel) }

