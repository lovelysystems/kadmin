package ls

import io.micronaut.configuration.picocli.PicocliRunner
import jakarta.inject.Inject
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.util.*

@Command(name = "kadmin", mixinStandardHelpOptions = true)
class KadminCommand() : Runnable {

    @Inject
    lateinit var admin: Admin

    @Command(name = "log-dirs", description = ["shows log dir info"])
    fun logDirs() {
        admin.logDirSizes().forEach { (id, size) ->
            println("$id $size")
        }
    }

    @Command(
        name = "ensure-replicas",
        description = ["sets the number of replicas of matching partitions to at least the given number"]
    )
    fun ensureReplicas(
        num: Int,
        @Option(names = ["-r"], description = ["filter by number of replicas"])
        filterNumReplicas: Int? = null,
        @Option(names = ["-t"], description = ["filter topics by name"])
        filterName: String? = null,
        @Option(names = ["--apply"], description = ["apply the reassignments"])
        applyReassignments: Boolean = false
    ) {
        val ra = admin.ensureReplicas(num, filterNumReplicas, filterName)

        println("reassignments to be issued:")
        ra.forEach { (tp, a) ->
            println("$tp -> ${a.targetReplicas()}")
        }

        if (!applyReassignments) {
            return
        }
        println("issuing reassignments ...")
        val res = admin.admin.alterPartitionReassignments(ra.mapValues { Optional.of(it.value) })
        res.values().forEach { t, u ->
            println("$t -> ${u.get()}")
        }
    }

    @Command(description = ["lists topic partitions"])
    fun list(
        @Option(names = ["-r"], description = ["filter by number of replicas"])
        filterNumReplicas: Int? = null,
        @Option(names = ["-t"], description = ["filter topics by name"])
        filterName: String? = null,
    ) {
        admin.partitions(filterNumReplicas, filterName).forEach { (t, p) ->
            println("${t.name()} ${p.partition()} -> ${p.replicas().map { it.id() }}")
        }
    }

    override fun run() {
        println("please specify a sub command")
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            PicocliRunner.run(KadminCommand::class.java, *args)
        }
    }
}
