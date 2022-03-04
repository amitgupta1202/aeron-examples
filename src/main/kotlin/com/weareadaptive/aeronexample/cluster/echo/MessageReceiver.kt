package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.cluster.client.EgressListener
import io.aeron.logbuffer.Header
import org.agrona.DirectBuffer

internal class MessageReceiver : EgressListener {
    override fun onMessage(
        clusterSessionId: Long,
        timestamp: Long,
        buffer: DirectBuffer,
        offset: Int,
        length: Int,
        header: Header
    ) {
        println("Received: ${buffer.getLong(offset)}")
    }
}