package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.cluster.client.AeronCluster
import io.aeron.driver.MediaDriver
import io.aeron.driver.ThreadingMode

fun main() {
    val ingressEndpoints = "0=localhost:9000,1=localhost:9100,2=localhost=9200,"
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