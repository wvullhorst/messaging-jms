package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import arrow.core.getOrElse
import arrow.core.recoverWith
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

fun loopUntilShutdown(shutDownSignal: () -> Boolean,
                      body: () -> Try<Unit>): Try<Unit> {
    while (!shutDownSignal.invoke()) {
        body.invoke()
                .getOrElse {
                    logger.debug { "loopUntilShutdown: error in invocation: ${it.message}, sleep..." }
                    Thread.sleep(1000)
                }
        logger.debug("loopUntilShutdown: loop again")
    }
    logger.debug("loopUntilShutdown stopped")
    return Try.just(Unit)
}

fun retryForever(delayInSeconds: Int = 1,
                 shutDownSignal: () -> Boolean,
                 body: () -> Try<Unit>): Try<Unit> {
    while (true) {
        if (body.invoke().isSuccess())
            return Try.just(Unit)
        if (shutDownSignal.invoke()) {
            logger.info("retryForever done, shutdown in progress")
            return Try.just(Unit)
        }
        Thread.sleep(delayInSeconds * 1000L)
    }
}

fun loopUntilFails(shutDownSignal: () -> Boolean,
                   body: () -> Try<Unit>): Try<Unit> {
    while (true) {
        if (body.invoke().isFailure())
            return Try.just(Unit)
        if (shutDownSignal.invoke()) {
            logger.info("retryForever done, shutdown in progress")
            return Try.just(Unit)
        }
    }
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

fun <T, A> closeAfterUsage(id: String,
                           creator: () -> Try<T>,
                           destroyer: (T) -> Try<Unit>,
                           body: (T) -> Try<A>): Try<A> =
        creator.invoke()
                .andThen { instance ->
                    body.invoke(instance)
                            .andThen { result ->
                                logger.warn("$id: invocation finished normally, destroy")
                                destroyer.invoke(instance).map { result }
                            }
                            .recoverWith { error ->
                                logger.warn("$id: invocation caused failure: ${error.message}, destroy")
                                destroyer.invoke(instance)
                                Try.raise(error)
                            }
                }

fun <T, A> invalidateOnFailure(id: String,
                               creator: () -> Try<T>,
                               invalidator: () -> Try<Unit>,
                               body: (T) -> Try<A>): Try<A> =
        creator.invoke()
                .andThen { instance ->
                    body.invoke(instance)
                            .recoverWith { error ->
                                logger.warn("$id: invocation caused failure: $error.message, invalidate")
                                invalidator.invoke()
                                Try.raise(error)
                            }
                }
