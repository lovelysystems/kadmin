package ls

import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.TopicPartitionInfo
import kotlin.math.pow

fun TopicPartitionInfo.topicPartition(topic: String): TopicPartition {
    return TopicPartition(topic, partition())
}

@Singleton
class Admin(val admin: AdminClient) {

    private inner class Reassigner(val numReplicas: Int) {

        val partitionSizes = mutableMapOf<TopicPartition, Long>()
        val nodes = admin.describeCluster().nodes().get().map { it.id() }
        val nodeSizes = admin.describeLogDirs(nodes).allDescriptions().get().map { (id, descriptions) ->
            val size = descriptions.map {
                it.value.replicaInfos().map {
                    partitionSizes[it.key] = it.value.size()
                    it.value.size()
                }.sum()
            }.sum()
            Pair(id, size) // println("$id $size")
        }.toMap().toMutableMap()

        private fun nextBrokerId(exclude: Collection<Int>): Int {
            return nodeSizes.filter { !exclude.contains(it.key) }.minByOrNull { it.value }?.key
                ?: error("no more brokers found. excluded: ${exclude.joinToString()}")
        }

        private fun computeReassignment(
            desc: TopicDescription,
            tpi: TopicPartitionInfo
        ): Pair<TopicPartition, NewPartitionReassignment>? {
            val targets = tpi.replicas().map { it.id() }.toMutableList()
            if (targets.size >= numReplicas) return null

            val tp = TopicPartition(desc.name(), tpi.partition())
            val size = partitionSizes[tp] ?: error { "size for partition not found $tp" }
            while (targets.size < numReplicas) {
                val addedBroker = nextBrokerId(targets)
                targets += addedBroker
                val newSizeAfterReassign = nodeSizes[addedBroker]!! + (size)
                nodeSizes[addedBroker] = newSizeAfterReassign
            }
            return Pair(tp, NewPartitionReassignment(targets))
        }

        fun computeReassignments(
            descs: Sequence<Pair<TopicDescription, TopicPartitionInfo>>,
            limit: Int,
        ): Map<TopicPartition, NewPartitionReassignment> {
            // add the big partitions first because the cluster is likely more uneven at the beginning of the computation
            val sorted =
                descs.sortedBy { -(partitionSizes[it.second.topicPartition(it.first.name())] ?: 0L) }.take(limit)
            val reassignments = sorted.map { computeReassignment(it.first, it.second) }.filterNotNull()
            return reassignments.toMap()
        }
    }

    fun logDirSizes(): Map<Int, Long> {
        val nodes = admin.describeCluster().nodes().get().map { it.id() }
        return admin.describeLogDirs(nodes).allDescriptions().get().map { (id, descriptions) ->
            val size = descriptions.map {
                it.value.replicaInfos().map {
                    it.value.size()
                }.sum()
            }.sum() / 1024.0.pow(3.0).toInt()
            Pair(id, size) // println("$id $size")
        }.toMap()
    }

    fun ensureReplicas(
        num: Int,
        limit: Int,
        filterNumReplicas: Int? = null,
        filterName: String? = null,
    ): Map<TopicPartition, NewPartitionReassignment> {
        val r = Reassigner(num)
        return r.computeReassignments(partitions(filterNumReplicas, filterName), limit)
    }

    fun partitions(filterNumReplicas: Int? = null, filterName: String? = null) =
        sequence<Pair<TopicDescription, TopicPartitionInfo>> {
            val nameFilter: (String) -> Boolean = if (filterName != null) {
                { s -> s.startsWith(filterName) }
            } else {
                { true }
            }
            val partFilter: (TopicPartitionInfo) -> Boolean = if (filterNumReplicas != null) {
                { pi -> pi.replicas().size == filterNumReplicas }
            } else {
                { true }
            }
            val names = admin.listTopics().names().get().filter(nameFilter)

            admin.describeTopics(names).values().forEach { (_, f) ->
                val desc = f.get()
                desc.partitions().filter(partFilter).forEach { pi ->
                    yield(Pair(desc, pi))
                }
            }
        }
}