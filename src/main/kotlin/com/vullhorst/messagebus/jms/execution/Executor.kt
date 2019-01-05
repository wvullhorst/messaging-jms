package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import arrow.core.recoverWith
import com.vullhorst.messagebus.jms.model.ShutDownSignal
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

fun retryForever(shutDownSignal: ShutDownSignal,
                 delayInSeconds: Int = 1,
                 body: () -> Try<Unit>) {
    while(!shutDownSignal.signal && body.invoke().isFailure()) {
        Thread.sleep(delayInSeconds * 1000L)
    }
}

fun retryOnce(shutDownSignal: ShutDownSignal,
              body: () -> Try<Unit>): Try<Unit> =
        retry(shutDownSignal, 1) { body() }

fun retry(shutDownSignal: ShutDownSignal,
          numberOfRetries: Int = 2,
          delay: Long = 1,
          delayUnit: TimeUnit = TimeUnit.SECONDS,
          body: () -> Try<Unit>): Try<Unit> {
    if(!shutDownSignal.signal) {
        return body.invoke()
                .recoverWith {
                    if (numberOfRetries > 0) {
                        logger.warn("error occurred: ${it.message}, retrying after $delay $delayUnit...")
                        invokeAfterDelay(delay, delayUnit) {
                            retry(shutDownSignal,
                                    numberOfRetries - 1,
                                    delay,
                                    delayUnit,
                                    body)
                        }
                    } else {
                        logger.warn("retries failed")
                        Try.raise(it)
                    }
                }
    }
    else {
        return Try.just(Unit)
    }
}

private fun invokeAfterDelay(delay: Long, unit: TimeUnit, body: () -> Try<Unit>): Try<Unit> {
    unit.sleep(delay)
    return body.invoke()
}

fun runAfterDelay(delay: Long, unit: TimeUnit, body: () -> Unit) {
    unit.sleep(delay)
    return body.invoke()
}
