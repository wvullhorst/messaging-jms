package com.vullhorst.messagebus.jms.model

import arrow.core.Try
import com.google.gson.Gson
import java.util.*
import javax.jms.IllegalStateException
import javax.jms.Message
import javax.jms.Session
import javax.jms.TextMessage

data class Event(
        val eventId: EventId,
        val occurredOn: Date
)

fun Event.asText() = Try { Gson().toJson(this)!! }

fun Event.serialize(session: Session): Try<Message> = this.asText().map(session::createTextMessage)

fun Message.deserialize(): Try<Event> {
    return when (this) {
        is TextMessage -> Try { Gson().fromJson(this.text, Event::class.java) }
        else -> Try.raise(IllegalStateException("message is not of type textMessage"))
    }
}
