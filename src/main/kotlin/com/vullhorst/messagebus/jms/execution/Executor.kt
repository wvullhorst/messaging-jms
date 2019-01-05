package com.vullhorst.messagebus.jms.execution

import arrow.core.Try
import arrow.core.recoverWith
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

fun retryForever(delayInSeconds: Int = 1,
                 body: () -> Try<Unit>) {
    while (body.invoke().isFailure()) {
        Thread.sleep(delayInSeconds * 1000L)
    }
}

fun retryOnce(body: () -> Try<Unit>): Try<Unit> =
        retry(1) { body() }

fun retry(numberOfRetries: Int = 2,
          delay: Long = 1,
          delayUnit: TimeUnit = TimeUnit.SECONDS,
          body: () -> Try<Unit>): Try<Unit> = body.invoke()
        .recoverWith {
            if (numberOfRetries > 0) {
                logger.warn("error occurred: ${it.message}, retrying after $delay $delayUnit...")
                invokeAfterDelay(delay, delayUnit) {
                    retry(numberOfRetries - 1,
                            delay,
                            delayUnit,
                            body)
                }
            } else {
                logger.warn("retries failed")
                Try.raise(it)
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
