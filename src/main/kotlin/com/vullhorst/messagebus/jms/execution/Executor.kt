package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit

class ExecutorException(message: String) : Exception(message)

val shutdownInProgressTry = Try.raise<Unit>(ExecutorException("shutdown in progress"))

fun loopUntilShutdown(shutDownSignal: () -> Boolean,
                      body: () -> Try<Unit>): Try<Unit> {
    while (!shutDownSignal.invoke()) body.invoke()
    return shutdownInProgressTry
}

fun retryForever(delayInSeconds: Int = 1,
                 shutDownSignal: () -> Boolean,
                 body: () -> Try<Unit>): Try<Unit> {
    while (!shutDownSignal.invoke() && !body.invoke().isFailure()) {
        Thread.sleep(delayInSeconds * 1000L)
    }
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

fun Executor.execute(numberOfThreads: Int,
                     threadNameBuilder: (Int) -> String,
                     body: () -> Unit) =
        (1..numberOfThreads).forEach { consumerId ->
            this.execute {
                Thread.currentThread().name = threadNameBuilder.invoke(consumerId)
                body.invoke()
            }
        }
