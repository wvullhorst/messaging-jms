package com.vullhorst.messagebus.jms

import arrow.core.Try
import com.vullhorst.messagebus.jms.execution.runAfterDelay
import com.vullhorst.messagebus.jms.model.Channel
import mu.KotlinLogging
import org.apache.activemq.ActiveMQConnectionFactory
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.jms.Message
import javax.jms.TextMessage

class DispatcherTest {
    private val logger = KotlinLogging.logger {}

    private val dispatcherTopic = Channel.Topic("dispatchTopic", "dispatcher")
    private val dispatcherQueue = Channel.Queue("dispatchQueue")
    private val senderTopic = Channel.Topic("dispatchTopic", "sender")
    private val receiverQueue = Channel.Queue("dispatchQueue")

    private fun dispatcher(): Dispatcher {
        val factory = ActiveMQConnectionFactory("tcp://syn1:61616")
        factory.clientID = "dispatcher"
        return Dispatcher({ factory },
                dispatcherTopic,
                dispatcherQueue,
                { session, message ->
                    Try {
                        val messageText = (message as TextMessage).text
                        val outgoingMessage = session.createTextMessage("$messageText - dispatched")
                        logger.debug("groupId: $messageText")
                        outgoingMessage.setStringProperty("JMSXGroupID", messageText)
                        outgoingMessage
                    }
                })
    }

    @Test
    fun dispatcherDispatchesSimpleMessage() {
        val done = CompletableFuture<String>()
        dispatcher().startup()
        val factory = ActiveMQConnectionFactory("tcp://syn1:61616")
        factory.clientID = "messagebus"
        val messageBus = MessageBus<String>({ factory },
                { session, str ->
                    Try {
                        session.createTextMessage(str)
                    }
                },
                { message: Message ->
                    Try {
                        when (message) {
                            is TextMessage -> message.text
                            else -> throw IllegalStateException("invalid message type")
                        }
                    }
                })
        messageBus.receive(receiverQueue) { msg ->
            done.complete(msg)
            Try.just(Unit)
        }
        runAfterDelay(5, TimeUnit.SECONDS) {
            messageBus.send(senderTopic, "simple message")
            done.get()
        }
    }

    @Test
    fun dispatcherDispatchesMultipleSimpleMessages() {
        dispatcher().startup()
        val factory = ActiveMQConnectionFactory("tcp://syn1:61616")
        factory.clientID = "messagebus"
        val messageBus = MessageBus<String>({ factory },
                { session, str ->
                    Try {
                        session.createTextMessage(str)
                    }
                },
                { message: Message ->
                    Try {
                        when (message) {
                            is TextMessage -> message.text
                            else -> throw IllegalStateException("invalid message type")
                        }
                    }
                })
        val latch = CountDownLatch(10)
        messageBus.receive(receiverQueue) {
            latch.countDown()
            Try.just(Unit)
        }
        runAfterDelay(5, TimeUnit.SECONDS) {
            (1..10).forEach {
                messageBus.send(senderTopic, "simple message $it")
            }
            latch.await()
        }
    }

    @Test
    fun messagesWithHeaderAreDeliveredToSameConsumer() {
        dispatcher().startup()
        val factory = ActiveMQConnectionFactory("tcp://syn1:61616")
        factory.clientID = "messagebus"
        val messageBus = MessageBus<String>({ factory },
                { session, str ->
                    Try {
                        session.createTextMessage(str)
                    }
                },
                { message: Message ->
                    Try {
                        println("dispatch-receiver: got $message")
                        when (message) {
                            is TextMessage -> message.text
                            else -> throw IllegalStateException("invalid message type")
                        }
                    }
                })
        val messageCount = 100
        val receiverCount = 10
        val messageToThreadSetMap = HashMap<String, MutableSet<String>>()
        val latch = CountDownLatch(messageCount)
        messageBus.receive(receiverQueue, receiverCount) { msg ->
            logger.debug("${Thread.currentThread()} bus receiver: $msg")
            addThreadToResult(messageToThreadSetMap, msg)
            latch.countDown()
            Try.just(Unit)
        }
        runAfterDelay(2, TimeUnit.SECONDS) {
            (1..messageCount).forEach {
                messageBus.send(senderTopic, "${it % 10}")
            }
            latch.await()
            logger.debug("all events received, result=$messageToThreadSetMap")
            Assert.assertEquals(receiverCount, messageToThreadSetMap.size)
        }
    }

    private fun addThreadToResult(messageToThreadSetMap: HashMap<String, MutableSet<String>>, message: String) {
        val threadName = Thread.currentThread().name
        var set = messageToThreadSetMap[threadName]
        if (set == null) {
            set = mutableSetOf()
            messageToThreadSetMap[threadName] = set
        }
        messageToThreadSetMap[threadName]!!.add(message)
    }
}