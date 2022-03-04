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

class AeronServerNode(
    private val service: ClusteredService,
    private val nodeId: Int,
    private val hostnames: List<String> = listOf("localhost", "localhost", "localhost"),
    private val clientPorts: List<Int> = listOf(9000, 9100, 9200),
    private val archiveControlPortOffset: Int = 1,
    private val memberPortOffset: Int = 2,
    private val logPortOffset: Int = 3,
    private val transferPortOffset: Int = 4,
    private val logControlPortOffset: Int = 5
) {
    init {
        require(nodeId in 0..2) { "invalid nodeId" }
        require(hostnames.size == 3) { "invalid number of hostnames" }
        require(clientPorts.size == 3) { "invalid number of hostnames" }
    }

    private val hostname: String
        get() = hostnames[nodeId]

    fun start() {
        val baseDir = File(System.getProperty("user.dir") + "/data", "node$nodeId")
        val aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver"
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

        val controlChannel = ChannelUriStringBuilder()
            .media("udp")
            .termLength(TERM_LENGTH)
            .endpoint("$hostname:${clientPorts[nodeId] + archiveControlPortOffset}")
            .build()

        val archiveContext = Archive.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveDir(File(baseDir, "archive"))
            .controlChannel(controlChannel)
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

        val logChannel = ChannelUriStringBuilder()
            .media("udp")
            .termLength(TERM_LENGTH)
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .controlEndpoint("$hostname:${clientPorts[nodeId] + logControlPortOffset}")
            .build()

        val replicationChannel = ChannelUriStringBuilder()
            .media("udp")
            .endpoint("$hostname:0")
            .build()

        val clusterMembers = clusterMembers(hostnames, clientPorts)

        val consensusModuleContext = ConsensusModule.Context()
            .clusterMemberId(nodeId)
            .clusterMembers(clusterMembers)
            .clusterDir(File(baseDir, "cluster"))
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel(logChannel)
            .replicationChannel(replicationChannel)
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

    private fun clusterMembers(hostnames: List<String>, clientPorts: List<Int>): String {
        fun StringBuilder.appendPort(hostname: String, port: Int) = this
            .append(',')
            .append(hostname)
            .append(':')
            .append(port)

        return hostnames.foldIndexed(StringBuilder()) { i, sb, hostname ->
            sb.append(i)
            sb.appendPort(hostname, clientPorts[i])
            sb.appendPort(hostname, clientPorts[i] + memberPortOffset)
            sb.appendPort(hostname, clientPorts[i] + logPortOffset)
            sb.appendPort(hostname, clientPorts[i] + transferPortOffset)
            sb.appendPort(hostname, clientPorts[i] + archiveControlPortOffset)
            sb.append('|')
        }.toString()
    }

    private fun errorHandler(context: String) = ErrorHandler { throwable: Throwable ->
        System.err.println(context)
        throwable.printStackTrace(System.err)
    }

    companion object {
        private const val TERM_LENGTH = 64 * 1024
    }
}

object Node0 {
    private val node = AeronServerNode(EchoClusteredService(), 0)

    @JvmStatic
    fun main(args: Array<String>) {
        node.start()
    }
}

object Node1 {
    private val node = AeronServerNode(EchoClusteredService(), 1)

    @JvmStatic
    fun main(args: Array<String>) {
        node.start()
    }
}

object Node2 {
    private val node = AeronServerNode(EchoClusteredService(), 2)

    @JvmStatic
    fun main(args: Array<String>) {
        node.start()
    }
}

