package ls

import io.micronaut.configuration.picocli.PicocliRunner
import jakarta.inject.Inject
import picocli.CommandLine.*
import java.util.*

/**
 * Provides the version from the manifest.
 * TODO: version info is not available in native builds
 */
class VersionProvider : IVersionProvider {
    override fun getVersion(): Array<String> {
        return arrayOf(javaClass.`package`.implementationVersion)
    }
}

class PartitionFilterOptions {

    @Option(names = ["-r"], description = ["filter by number of replicas"])
    var filterNumReplicas: Int? = null

    @Option(names = ["-t"], description = ["filter topics by name"])
    var filterName: String? = null
}

@Command(
    name = "kadmin",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider::class,
    showDefaultValues = true
)
class KadminCommand : Runnable {

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
        description = ["sets the number of replicas of matching partitions to at least the given number"],
        showDefaultValues = true,
    )
    fun ensureReplicas(
        num: Int,
        @Option(names = ["-l"], description = ["limit the maximum number of reassignments to add"], defaultValue = "5")
        limit: Int,
        @Mixin filters: PartitionFilterOptions,
        @Option(names = ["--apply"], description = ["apply the reassignments"])
        applyReassignments: Boolean = false,
    ) {
        val ra = admin.ensureReplicas(num, limit, filters.filterNumReplicas, filters.filterName)

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

    @Command(description = ["lists topic partitions"], showDefaultValues = true)
    fun list(@Mixin filterOptions: PartitionFilterOptions) {
        admin.partitions(filterOptions.filterNumReplicas, filterOptions.filterName).forEach { (t, p) ->
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
