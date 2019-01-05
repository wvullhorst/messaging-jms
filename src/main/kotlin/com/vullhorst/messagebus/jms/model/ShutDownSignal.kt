package com.vullhorst.messagebus.jms.model

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class ShutDownSignal {
    private var signal = false

    fun repeatUntilShutDown(body: () -> Unit) {
        while (!isSet()) body.invoke()
    }

    fun repeatUntilShutDown(additionalCondition: () -> Boolean, body: () -> Unit) {
        while (!isSet() && additionalCondition.invoke()) body.invoke()
    }

    fun set() {
        this.signal = true
    }

    private fun isSet(): Boolean {
        if(this.signal) logger.warn("shutDown requested")
        return this.signal
    }
}