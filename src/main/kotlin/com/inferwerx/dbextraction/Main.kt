package com.inferwerx.dbextraction

import java.io.IOException
import com.github.salomonbrys.kotson.*
import com.google.gson.Gson
import com.inferwerx.dbextraction.config.DatabaseImport
import com.inferwerx.dbextraction.config.ExportDbConfiguration
import com.inferwerx.dbextraction.config.ImportConfig
import com.inferwerx.dbextraction.config.PostRunScript
import java.io.File
import java.io.FileReader
import java.sql.*
import java.text.NumberFormat

object Main {
    @Throws(IOException::class)
    @JvmStatic fun main(args: Array<String>) {
        var exportDbFile = "settings.json"
        var knownDbFile = "known-databases.json"

        if (args.count() >= 2) {
            exportDbFile = args[0]
            knownDbFile = args[1]
        }

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