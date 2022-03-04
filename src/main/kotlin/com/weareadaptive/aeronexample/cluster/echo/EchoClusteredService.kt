package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.ExclusivePublication
import io.aeron.Image
import io.aeron.cluster.codecs.CloseReason
import io.aeron.cluster.service.ClientSession
import io.aeron.cluster.service.Cluster
import io.aeron.cluster.service.ClusteredService
import io.aeron.logbuffer.Header
import org.agrona.DirectBuffer
import org.agrona.concurrent.IdleStrategy

internal class EchoClusteredService : ClusteredService {
    private lateinit var cluster: Cluster
    private lateinit var idleStrategy: IdleStrategy
    override fun onStart(cluster: Cluster, snapshotImage: Image?) {
        this.cluster = cluster
        idleStrategy = cluster.idleStrategy()
    }

    override fun onSessionOpen(session: ClientSession, timestamp: Long) {
        println("onSessionOpen($session)")
    }

    override fun onSessionClose(session: ClientSession, timestamp: Long, closeReason: CloseReason) {
        println("onSessionClose($session)")
    }

    override fun onSessionMessage(
        session: ClientSession,
        timestamp: Long,
        buffer: DirectBuffer,
        offset: Int,
        length: Int,
        header: Header
    ) {
        val message = buffer.getStringAscii(offset)
        println("received message: $message")

        idleStrategy.reset()
        while (session.offer(buffer, offset, length) < 0) {
            idleStrategy.idle()
        }
    }

    override fun onTakeSnapshot(snapshotPublication: ExclusivePublication) {}
    override fun onTimerEvent(correlationId: Long, timestamp: Long) {}
    override fun onRoleChange(newRole: Cluster.Role) {}
    override fun onTerminate(cluster: Cluster) {}
}

