package com.vullhorst.messagebus.jms.model

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

data class ShutDownSignal(var signal:Boolean = false)
