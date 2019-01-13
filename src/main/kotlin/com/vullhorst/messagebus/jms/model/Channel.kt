package com.vullhorst.messagebus.jms.model

import arrow.core.Try
import javax.jms.Destination
import javax.jms.Session

sealed class Channel {
    data class Topic(val id: String, val consumerId: String) : Channel()
    data class Queue(val id: String, val consumerId: String = "") : Channel()
}

fun Channel.consumerName() = when(this) {
    is Channel.Topic -> this.consumerId
    is Channel.Queue -> this.consumerId
}

fun createDestination(session: Session,
                      channel: Channel): Try<Destination> {
    return Try {
        when (channel) {
            is Channel.Topic -> session.createTopic(channel.id)
            is Channel.Queue -> session.createQueue(channel.id)
        }
    }
}
