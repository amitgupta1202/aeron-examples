package com.weareadaptive.aeronexample.cluster.echo

import io.aeron.ChannelUriStringBuilder
import io.aeron.CommonContext
import io.aeron.archive.Archive
import io.aeron.archive.ArchiveThreadingMode
import io.aeron.archive.client.AeronArchive
import io.aeron.cluster.ClusteredMediaDriver
import io.aeron.cluster.ConsensusModule
import io.aeron.cluster.service.ClusteredService
import io.aeron.cluster.service.ClusteredServiceContainer
import io.aeron.driver.MediaDriver
import io.aeron.driver.MinMulticastFlowControlSupplier
import io.aeron.driver.ThreadingMode
import org.agrona.ErrorHandler
import org.agrona.concurrent.NoOpLock
import org.agrona.concurrent.ShutdownSignalBarrier
import java.io.File

/*
 Logic shared between client and server to know the port number
 */
const val CLIENT_FACING_PORT_OFFSET = 2
fun calculatePort(nodeId: Int, offset: Int) = PORT_BASE + nodeId * PORTS_PER_NODE + offset


private const val PORT_BASE = 9000
private const val PORTS_PER_NODE = 100
private const val TERM_LENGTH = 64 * 1024
private const val ARCHIVE_CONTROL_PORT_OFFSET = 1
private const val MEMBER_FACING_PORT_OFFSET = 3
private const val LOG_PORT_OFFSET = 4
private const val TRANSFER_PORT_OFFSET = 5
private const val LOG_CONTROL_PORT_OFFSET = 6

@Suppress("SameParameterValue")
private fun udpChannel(nodeId: Int, hostname: String, portOffset: Int) =
    calculatePort(nodeId, portOffset).let { port ->
        ChannelUriStringBuilder()
            .media("udp")
            .termLength(TERM_LENGTH)
            .endpoint("$hostname:$port")
            .build()
    }

@Suppress("SameParameterValue")
private fun logControlChannel(nodeId: Int, hostname: String, portOffset: Int) =
    calculatePort(nodeId, portOffset).let { port ->
        ChannelUriStringBuilder()
            .media("udp")
            .termLength(TERM_LENGTH)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .controlEndpoint("$hostname:$port")
            .build()
    }

private fun logReplicationChannel(hostname: String) = ChannelUriStringBuilder()
    .media("udp")
    .endpoint("$hostname:0")
    .build()

private fun clusterMembers(hostnames: List<String>): String {
    fun StringBuilder.appendPort(nodeId: Int, hostname: String, port: Int) = this
        .append(',')
        .append(hostname)
        .append(':')
        .append(calculatePort(nodeId, port))

    return hostnames.foldIndexed(StringBuilder()) { i, sb, hostname ->
        sb.append(i)
        sb.appendPort(i, hostname, CLIENT_FACING_PORT_OFFSET)
        sb.appendPort(i, hostname, MEMBER_FACING_PORT_OFFSET)
        sb.appendPort(i, hostname, LOG_PORT_OFFSET)
        sb.appendPort(i, hostname, TRANSFER_PORT_OFFSET)
        sb.appendPort(i, hostname, ARCHIVE_CONTROL_PORT_OFFSET)
        sb.append('|')
    }.toString()
}

private fun errorHandler(context: String) = ErrorHandler { throwable: Throwable ->
    System.err.println(context)
    throwable.printStackTrace(System.err)
}

private fun startServer(nodeId: Int, service: ClusteredService) {
    val hostnames = listOf("localhost", "localhost", "localhost")
    val baseDir = File(System.getProperty("user.dir"), "node$nodeId")
    val aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver"
    val hostname: String = hostnames[nodeId]
    val barrier = ShutdownSignalBarrier()

    val mediaDriverContext = MediaDriver.Context()
        .aeronDirectoryName(aeronDirName)
        .threadingMode(ThreadingMode.SHARED)
        .termBufferSparseFile(true)
        .multicastFlowControlSupplier(MinMulticastFlowControlSupplier())
        .terminationHook(barrier::signal)
        .errorHandler(errorHandler("Media Driver"))

    val replicationArchiveContext = AeronArchive.Context()
        .controlResponseChannel("aeron:udp?endpoint=$hostname:0")
        .errorHandler(errorHandler("Replication Archiver"))

    val archiveContext = Archive.Context()
        .aeronDirectoryName(aeronDirName)
        .archiveDir(File(baseDir, "archive"))
        .controlChannel(udpChannel(nodeId, hostname, 1))
        .archiveClientContext(replicationArchiveContext)
        .localControlChannel("aeron:ipc?term-length=64k")
        .recordingEventsEnabled(false)
        .threadingMode(ArchiveThreadingMode.SHARED)
        .errorHandler(errorHandler("Archiver"))

    val aeronArchiveContext = AeronArchive.Context()
        .lock(NoOpLock.INSTANCE)
        .controlRequestChannel(archiveContext.localControlChannel())
        .controlRequestStreamId(archiveContext.localControlStreamId())
        .controlResponseChannel(archiveContext.localControlChannel())
        .aeronDirectoryName(aeronDirName)

    val consensusModuleContext = ConsensusModule.Context()
        .clusterMemberId(nodeId)
        .clusterMembers(clusterMembers(hostnames))
        .clusterDir(File(baseDir, "cluster"))
        .ingressChannel("aeron:udp?term-length=64k")
        .logChannel(logControlChannel(nodeId, hostname, LOG_CONTROL_PORT_OFFSET))
        .replicationChannel(logReplicationChannel(hostname))
        .archiveContext(aeronArchiveContext.clone())
        .errorHandler(errorHandler("Consensus Module"))

    val clusteredServiceContext = ClusteredServiceContainer.Context()
        .aeronDirectoryName(aeronDirName)
        .archiveContext(aeronArchiveContext.clone())
        .clusterDir(File(baseDir, "cluster"))
        .clusteredService(service)
        .errorHandler(errorHandler("Clustered Service"))

    ClusteredMediaDriver.launch(mediaDriverContext, archiveContext, consensusModuleContext).use {
        ClusteredServiceContainer.launch(clusteredServiceContext).use {
            println("[$nodeId] Started Cluster Node on $hostname...")
            barrier.await()
            println("[$nodeId] Exiting")
        }
    }
}

object Node0 {
    @JvmStatic
    fun main(args: Array<String>) {
        startServer(0, EchoClusteredService())
    }
}

object Node1 {
    @JvmStatic
    fun main(args: Array<String>) {
        startServer(1, EchoClusteredService())
    }
}

object Node2 {
    @JvmStatic
    fun main(args: Array<String>) {
        startServer(2, EchoClusteredService())
    }
}

