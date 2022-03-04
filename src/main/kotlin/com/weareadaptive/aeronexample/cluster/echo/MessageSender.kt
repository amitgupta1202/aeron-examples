package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.cluster.client.AeronCluster
import org.agrona.BitUtil
import org.agrona.BitUtil.SIZE_OF_LONG
import org.agrona.ExpandableArrayBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.BackoffIdleStrategy
import org.agrona.concurrent.IdleStrategy
import java.util.concurrent.ThreadLocalRandom
import kotlin.random.Random

internal class MessageSender(private val cluster: AeronCluster) {
    private val buffer: MutableDirectBuffer = ExpandableArrayBuffer()
    private val idleStrategy: IdleStrategy = BackoffIdleStrategy()

    private fun sendMessage(message: Long) {
        buffer.putLong(0, message)
        idleStrategy.reset()
        while (cluster.offer(buffer, 0, SIZE_OF_LONG) < 0) {
            idleStrategy.idle(cluster.pollEgress())
        }
    }

    fun sendMessages(random: Random = Random(100), count: Int = 10) {
        var keepAliveDeadlineMs: Long = 0
        var nextMessageDeadlineMs = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(1000)
        var messagesLeftToSend = count
        while (!Thread.currentThread().isInterrupted) {
            val currentTimeMs = System.currentTimeMillis()
            if (nextMessageDeadlineMs <= currentTimeMs && messagesLeftToSend > 0) {
                val message = random.nextLong()
                println("sending message.... $message")
                sendMessage(message)
                nextMessageDeadlineMs = currentTimeMs + ThreadLocalRandom.current().nextInt(100)
                keepAliveDeadlineMs = currentTimeMs + 1000
                --messagesLeftToSend
            } else if (keepAliveDeadlineMs <= currentTimeMs) {
                keepAliveDeadlineMs =
                    if (messagesLeftToSend > 0) {
                        cluster.sendKeepAlive()
                        currentTimeMs + 1000
                    } else {
                        break
                    }
            }
            idleStrategy.idle(cluster.pollEgress())
        }
    }

}