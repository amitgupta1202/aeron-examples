package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.cluster.client.AeronCluster
import org.agrona.ExpandableArrayBuffer
import org.agrona.MutableDirectBuffer
import org.agrona.concurrent.BackoffIdleStrategy
import org.agrona.concurrent.IdleStrategy

internal class MessageSender(private val cluster: AeronCluster) {
    private val buffer: MutableDirectBuffer = ExpandableArrayBuffer()
    private val idleStrategy: IdleStrategy = BackoffIdleStrategy()

    private fun sendMessage(message: String) {
        val length = buffer.putStringAscii(0, message)
        idleStrategy.reset()
        while (cluster.offer(buffer, 0, length) < 0) {
            idleStrategy.idle(cluster.pollEgress())
        }
    }

    fun sendAndReceiveMessages(count: Int = 10) {
        for (i in 1..count) {
            val message = "Hello World ! ($i)"
            println("sending message.... $message")
            sendMessage(message)
            Thread.sleep(10) //allow time for message to come back, [DO NOT DO THIS IN REAL CODE]
            idleStrategy.idle(cluster.pollEgress()) //poll the egress if there is something to process
        }
    }

}
