package org.s2progger.dataflow

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import java.io.IOException
import com.github.salomonbrys.kotson.*
import com.google.gson.Gson
import org.s2progger.dataflow.config.ExportDbConfiguration
import org.s2progger.dataflow.config.ImportConfig
import java.io.FileReader

class Main : CliktCommand(name = "data-flow", help = "Export database tables") {
    val settings: String by option(help = "Specify a path to the settings.json file - default is to look in the same directory the application in run from", envvar = "DF_SETTINGS_FILE").default("settings.json")
    val databases: String by option(help = "Specify a path to the known-databases.json file - default is to look in the same directory the application in run from", envvar = "DF_DATABASES_FILE").default("known-databases.json")
    val application: String? by option(help = "The application from known-databases.json to select - if omitted a choice is given", envvar = "DF_SELECTED_APP")

    override fun run() {
        val gson = Gson()

        val exportDbConfig = gson.fromJson<ExportDbConfiguration>(FileReader(settings))
        val importConfig = gson.fromJson<ImportConfig>(FileReader(databases))

        val dbCopyUtil = DatabaseCopy(exportDbConfig);

        var selection = application

        if (selection == null) {
            println("Known databases")
            println()

            importConfig.knownDatabases.forEachIndexed { index, value ->
                println("[$index] - ${value.application}")
            }

            println()
            println("Enter a database # to copy: ")

            selection = readLine()
        }

        try {
            if (selection == null || selection.toIntOrNull() == null) {
                throw Throwable("'$selection' is not a valid number")
            }

            if (selection.toInt() < 0 || selection.toInt() >= importConfig.knownDatabases.count())
                throw Throwable("Selection out of range")

            importConfig.knownDatabases.forEachIndexed { index, value ->
                if (index == selection.toInt()) {
                    println(value.application)
                    println()

                    dbCopyUtil.copyDatabase(value.application, value.connectionDetails);
                }
            }
        } catch (e: Throwable){
            println("ERROR - ${e.message}")
        }

        println()
        println("All done! Hit enter to close")
        readLine()
    }
}


fun main(args: Array<String>) = Main().main(args)

/*
object Main {
    @Throws(IOException::class)
    @JvmStatic fun main(args: Array<String>) {
        var exportDbFile = "settings.json"
        var knownDbFile = "known-databases.json"
        var selectedApplication = ""

        if (args.count() > 2) {
            exportDbFile = args[0]
            knownDbFile = args[1]
            selectedApplication = args[2]
        } else if (args.count()
        )

        val gson = Gson()

        val exportDbConfig = gson.fromJson<ExportDbConfiguration>(FileReader(exportDbFile))
        val importConfig = gson.fromJson<ImportConfig>(FileReader(knownDbFile))

        val dbCopyUtil = DatabaseCopy(exportDbConfig);

        println("Known databases")
        println()

        importConfig.knownDatabases.forEachIndexed { index, value ->
            println("[$index] - ${value.application}")
        }

        println()
        println("Enter a database # to copy: ")
        val input = readLine()

        try {
            val selection = input!!.toInt()

            if (selection < 0 || selection >= importConfig.knownDatabases.count())
                throw Throwable("Selection out of range")

            importConfig.knownDatabases.forEachIndexed { index, value ->
                if (index == selection) {
                    println(value.application)
                    println()

                    dbCopyUtil.copyDatabase(value.application, value.connectionDetails);
                }
            }
        } catch (e: Throwable){
            println("ERROR - INVALID SELECTION")
        }

        println()
        println("All done! Hit enter to close")
        readLine()
    }


}
        */