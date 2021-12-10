package ls;

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.string.shouldContain
import io.micronaut.configuration.picocli.PicocliRunner
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.Environment
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class KadminCommandSpec : BehaviorSpec({

    given("kadmin") {
        val ctx = ApplicationContext.run(Environment.CLI, Environment.TEST)

        `when`("invocation with --help") {
            val baos = ByteArrayOutputStream()
            System.setOut(PrintStream(baos))

            val args = arrayOf("--help")
            PicocliRunner.run(KadminCommand::class.java, ctx, *args)

            then("should display usage info") {
                baos.toString() shouldContain "Usage: kadmin"
            }
        }

        ctx.close()
    }
})