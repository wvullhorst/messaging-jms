package com.vullhorst.messagebus.jms.io

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import javax.jms.MessageProducer

fun withProducer(context: DestinationContext,
                         body: (MessageProducer) -> Try<Unit>): Try<Unit> =
        Try { context.session.createProducer(context.destination) }
                .flatMap { body.invoke(it) }

