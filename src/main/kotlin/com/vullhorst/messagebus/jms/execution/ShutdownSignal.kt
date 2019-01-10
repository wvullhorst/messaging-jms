package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import com.vullhorst.messagebus.jms.io.handleIncomingMessages
import com.vullhorst.messagebus.jms.io.model.DestinationContext
import java.util.concurrent.TimeUnit
import javax.jms.Message

object ShutdownSignal {
    var inProgress: Boolean = false

    fun initialize() {
        ShutdownSignal.inProgress = false
    }
    fun set() {
        ShutdownSignal.inProgress = true
    }
}

fun loopUntilShutdown(body: () -> Unit) {
    while(!ShutdownSignal.inProgress) body.invoke()
}

fun retryForever(delayInSeconds: Int = 1,
                 body: () -> Try<Unit>) {
    retryForever(delayInSeconds,
            { ShutdownSignal.inProgress },
            body)
}

fun retry(numberOfRetries: Int = 2,
          delay: Long = 1,
          delayUnit: TimeUnit = TimeUnit.SECONDS,
          body: () -> Try<Unit>): Try<Unit> =
        retry(numberOfRetries,
                delay,
                delayUnit,
                { ShutdownSignal.inProgress },
                body)

fun <T> handleIncomingMessages(context: DestinationContext,
                               deserializer: (Message) -> Try<T>,
                               body: (T) -> Try<Unit>): Try<Unit> {
    return handleIncomingMessages(context,
            deserializer,
            { ShutdownSignal.inProgress },
            body)

}