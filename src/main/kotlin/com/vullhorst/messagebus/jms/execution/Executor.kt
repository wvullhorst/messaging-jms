package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import arrow.core.recoverWith
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

class ExecutorException(message:String):Exception (message)

fun retryForever(delayInSeconds: Int = 1,
                 shutDownSignal: () -> Boolean,
                 body: () -> Try<Unit>) {
    while (!shutDownSignal.invoke() && !body.invoke().isFailure()) {
        Thread.sleep(delayInSeconds * 1000L)
    }
}

/*
fun retryOnce(body: () -> Try<Unit>): Try<Unit> =
        retry(1) { body() }
*/

fun retry(numberOfRetries: Int = 2,
          delay: Long = 1,
          delayUnit: TimeUnit = TimeUnit.SECONDS,
          shutDownSignal: () -> Boolean,
          body: () -> Try<Unit>): Try<Unit> {

    if (!shutDownSignal.invoke()) {
        return body.invoke()
                .recoverWith {
                    if (numberOfRetries > 0) {
                        logger.warn("error occurred: ${it.message}, retrying after $delay $delayUnit...")
                        invokeAfterDelay(delay, delayUnit) {
                            retry(numberOfRetries - 1,
                                    delay,
                                    delayUnit,
                                    shutDownSignal,
                                    body)
                        }
                    } else {
                        logger.warn("retries failed")
                        Try.raise(it)
                    }
                }
    }
    else return Try.raise(ExecutorException("Shutdown in progress"))
}

private fun invokeAfterDelay(delay: Long, unit: TimeUnit, body: () -> Try<Unit>): Try<Unit> {
    unit.sleep(delay)
    return body.invoke()
}

fun runAfterDelay(delay: Long, unit: TimeUnit, body: () -> Unit) {
    unit.sleep(delay)
    return body.invoke()
}

fun <T, B> Try<T>.andThen(body: (T) -> Try<B>): Try<B> = this.flatMap(body)

fun <T, B> Try<T>.combineWith(body: (T) -> Try<B>): Try<Pair<T, B>> {
    return this.fold(
            { Try.raise(it) },
            { t -> body.invoke(t).map { b -> Pair(t, b) } })
}
