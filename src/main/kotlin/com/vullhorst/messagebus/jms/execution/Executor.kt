package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import arrow.core.getOrElse
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

class ExecutorException(message: String) : Exception(message)

val shutdownInProgressTry = Try.raise<Unit>(ExecutorException("shutdown in progress"))

fun loopUntilShutdown(shutDownSignal: () -> Boolean,
                      body: () -> Try<Unit>): Try<Unit> {
    while (!shutDownSignal.invoke()) {
        body.invoke()
                .getOrElse {
                    logger.debug { "loopUntilShutdown: error in invocation: ${it.message}, sleep..." }
                    Thread.sleep(1000)
                }
    }
    return Try.just(Unit)
}

fun retryForever(delayInSeconds: Int = 1,
                 shutDownSignal: () -> Boolean,
                 body: () -> Try<Unit>): Try<Unit> {
        while (body.invoke().isFailure()) {
            if(shutDownSignal.invoke())
                break
            logger.debug { "retryForever: sleep..." }
            Thread.sleep(delayInSeconds * 1000L)
        }
    logger.warn("shutdown requested, stopping loop")
    return shutdownInProgressTry
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