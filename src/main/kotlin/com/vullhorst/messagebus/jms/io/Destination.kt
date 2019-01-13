package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.andThen
import com.vullhorst.messagebus.jms.model.Channel
import com.vullhorst.messagebus.jms.model.createDestination
import javax.jms.Destination
import javax.jms.Session

fun onDestination(channel: Channel,
                  sessionProvider: () -> Session,
                  body: (Destination) -> Try<Unit>): Try<Unit> =
        sessionProvider.invoke().createDestination(channel)
                .andThen { body.invoke(it) }
