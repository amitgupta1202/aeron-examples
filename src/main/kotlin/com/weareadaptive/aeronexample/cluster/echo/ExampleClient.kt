package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.cluster.client.AeronCluster
import io.aeron.driver.MediaDriver
import io.aeron.driver.ThreadingMode

private fun Array<String>.asIngressEndpoints() = this
    .foldIndexed(StringBuilder()) { i, sb, hostname ->
        sb.append(i)
            .append('=')
            .append(hostname)
            .append(':')
            .append(calculatePort(i, CLIENT_FACING_PORT_OFFSET))
            .append(',')
    }.let {
        it.setLength(it.length - 1)
        it.toString()
    }

fun main() {
    val ingressEndpoints = arrayOf("localhost", "localhost", "localhost").asIngressEndpoints()
    val messageReceiver = MessageReceiver()

    val mediaDriverContext = MediaDriver.Context()
        .threadingMode(ThreadingMode.SHARED)
        .dirDeleteOnStart(true)
        .dirDeleteOnShutdown(true)
    val aeronClusterContext = AeronCluster.Context()
        .egressListener(messageReceiver)
        .egressChannel("aeron:udp?endpoint=localhost:0")
        .ingressChannel("aeron:udp")
        .ingressEndpoints(ingressEndpoints)
    MediaDriver.launchEmbedded(mediaDriverContext).use { mediaDriver ->
        aeronClusterContext.aeronDirectoryName(mediaDriver.aeronDirectoryName())
        AeronCluster.connect(aeronClusterContext).use { aeronCluster ->
            MessageSender(aeronCluster).sendMessages()
        }
    }
}
