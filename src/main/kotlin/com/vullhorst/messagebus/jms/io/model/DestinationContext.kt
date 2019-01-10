package com.vullhorst.messagebus.jms.io.model

import com.vullhorst.messagebus.jms.model.Channel
import javax.jms.Destination
import javax.jms.Session

data class DestinationContext(val session: Session,
                              val destination: Destination,
                              val channel: Channel)

