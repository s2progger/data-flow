package com.inferwerx.dbextraction

import java.io.IOException
import com.github.salomonbrys.kotson.*
import com.google.gson.Gson
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

        Class.forName(exportDbConfig.driver)

        if (exportDbConfig.outputFolder != null) {
            File(exportDbConfig.outputFolder).mkdirs()
        }

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

                    Class.forName(value.connectionDetails.driver)

                    val exportUrl = when (exportDbConfig.outputFolder.isNullOrEmpty()) {
                        true -> "${exportDbConfig.urlProtocol}${exportDbConfig.urlOptions}"
                        false -> "${exportDbConfig.urlProtocol}${exportDbConfig.outputFolder}${value.application.toLowerCase().replace(" ", "_")}-import${exportDbConfig.urlOptions}"
                    }

                    val importConnection = DriverManager.getConnection(value.connectionDetails.url, value.connectionDetails.username, value.connectionDetails.password)
                    val exportConnection = DriverManager.getConnection(exportUrl, exportDbConfig.username, exportDbConfig.password)


                    if (value.connectionDetails.sqlSetupCommands != null) {
                        val stmt = importConnection.createStatement()

                        stmt.execute(value.connectionDetails.sqlSetupCommands)
                        stmt.close()
                    }

                    if (exportDbConfig.sqlSetupCommands != null) {
                        val stmt = exportConnection.createStatement()

                        stmt.execute(exportDbConfig.sqlSetupCommands)
                        stmt.close()
                    }

                    importApplication(importConnection, exportConnection, value.connectionDetails.imports)

                    if (value.connectionDetails.postScripts != null)
                        runPostScripts(exportConnection, value.connectionDetails.postScripts)
                }
            }
        } catch (e: Throwable){
            println("ERROR - INVALID SELECTION")
        }

        println()
        println("All done! Hit enter to close")
        readLine()
    }

    private fun importApplication(importDbConnection: Connection, exportDbConnection: Connection, importList: List<DatabaseImport>) {
        importDbConnection.autoCommit = false
        exportDbConnection.autoCommit = false

        val importMeta = importDbConnection.metaData

        // Attempt to use a forward moving cursor for result sets in order to cut down on memory usage when fetching millions of rows
        val importStatement = importDbConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        val exportStatement = exportDbConnection.createStatement()

        System.out.println("Database product: ${importMeta.databaseProductName}")
        System.out.println("Database version: ${importMeta.databaseProductVersion}")

        for (import in importList) {
            System.out.println("Importing ${import.table}...")

            prepareImportTable(import.table, importStatement, exportStatement)

            importTable(import, importStatement, exportDbConnection)
        }

        importStatement.close()
        exportStatement.close()
    }

    private fun runPostScripts(connection: Connection, scripts: List<PostRunScript>) {
        val statement = connection.createStatement()

        for (script in scripts) {
            System.out.println("Running script: ${script.label}...")

            statement.executeUpdate(script.sql)

            System.out.println("${script.label} complete")
        }

        statement.close()
    }

    @Throws(Exception::class)
    private fun prepareImportTable(table: String, importStatement: Statement, exportStatement: Statement) {

        try {
            // Check if the table to import to already exists
            val tableDetectSql = "SELECT * FROM $table WHERE 1 = 2"

            exportStatement.executeQuery(tableDetectSql)
        } catch (e: Exception) {
            // Table doesn't exist, so create it
            val script = getTableCreateScript(table, importStatement)

            exportStatement.execute(script)
        }
    }

    @Throws(Exception::class)
    private fun getTableCreateScript(table: String, statement: Statement) : String {
        val script = StringBuffer()

        val tableDetectSql = "SELECT * FROM $table WHERE 1 = 2"
        val rs = statement.executeQuery(tableDetectSql)
        val meta = rs.metaData

        script.append("CREATE TABLE $table ( ")

        var first = true

        for (i in 1..meta.columnCount) {
            if (first) {
                first = false
            } else {
                script.append(", ")
            }

            val name = meta.getColumnName(i)
            val type = typeToTypeName(meta.getColumnType(i))
            val size = meta.getPrecision(i)
            val precision = meta.getScale(i)

            val nullable = if (meta.isNullable(i) == ResultSetMetaData.columnNoNulls) "NOT NULL" else ""

            if (isSizable(type) && isNumeric(type))
                script.append("$name $type ($size,$precision) $nullable")
            else if(isSizable(type))
                script.append("$name $type ($size) $nullable")
            else
                script.append("$name $type $nullable")
        }

        script.append(")")

        rs.close()

        return script.toString()
    }


    @Throws(Exception::class)
    private fun importTable(import: DatabaseImport, importStatement: Statement, exportConnection: Connection) {
        val columnDetectSql = "SELECT * FROM ${import.table} WHERE 1 = 2"
        val selectSql = import.query ?: "SELECT * FROM ${import.table}"

        val metaRs = importStatement.executeQuery(columnDetectSql)
        val meta = metaRs.metaData
        val columnTypes = Array(meta.columnCount, { -1 })

        for (i in 1..meta.columnCount) {
            columnTypes[i - 1] = meta.getColumnType(i)
        }

        val insertSql = "INSERT INTO ${import.table} VALUES (${setupParameterList(meta.columnCount)})"

        metaRs.close()

        importStatement.fetchSize = 10000

        val rs = importStatement.executeQuery(selectSql)
        val ps = exportConnection.prepareStatement(insertSql)

        var rowCount = 0

        while (rs.next()) {
            rowCount++

            for (i in 1..columnTypes.count()) {
                when (columnTypes[i - 1]) {
                    Types.ARRAY -> ps.setArray(i, rs.getArray(i))
                    Types.BIGINT -> ps.setLong(i, rs.getLong(i))
                    Types.BINARY -> ps.setBinaryStream(i, rs.getBinaryStream(i))
                    Types.BIT -> ps.setBoolean(i, rs.getBoolean(i))
                    Types.BLOB -> ps.setBlob(i, rs.getBlob(i))
                    Types.CLOB -> ps.setString(i, rs.getString(i))
                    Types.BOOLEAN -> ps.setBoolean(i, rs.getBoolean(i))
                    Types.CHAR -> ps.setString(i, rs.getString(i))
                    Types.DATE -> ps.setDate(i, rs.getDate(i))
                    Types.DECIMAL -> ps.setBigDecimal(i, rs.getBigDecimal(i))
                    Types.DOUBLE -> ps.setDouble(i, rs.getDouble(i))
                    Types.FLOAT -> ps.setFloat(i, rs.getFloat(i))
                    Types.INTEGER -> ps.setInt(i, rs.getInt(i))
                    Types.NCHAR -> ps.setString(i, rs.getString(i))
                    Types.NUMERIC -> ps.setBigDecimal(i, rs.getBigDecimal(i))
                    Types.NVARCHAR -> ps.setString(i, rs.getString(i))
                    Types.ROWID -> ps.setLong(i, rs.getLong(i))
                    Types.SMALLINT -> ps.setShort(i, rs.getShort(i))
                    Types.SQLXML -> ps.setString(i, rs.getString(i))
                    Types.TIME -> ps.setTime(i, rs.getTime(i))
                    Types.TIMESTAMP -> ps.setTimestamp(i, rs.getTimestamp(i))
                    Types.TINYINT -> ps.setByte(i, rs.getByte(i))
                    Types.VARBINARY -> ps.setBytes(i, rs.getBytes(i))
                    Types.VARCHAR -> ps.setString(i, rs.getString(i))
                    else -> ps.setString(i, rs.getString(i))
                }
            }

            ps.addBatch()

            if (rowCount % 10000 == 0) {
                ps.executeBatch()
                exportConnection.commit()

                System.out.print("\rExported ${NumberFormat.getInstance().format(rowCount)} records so far...")
                System.out.flush()
            }
        }

        ps.executeBatch()
        exportConnection.commit()

        ps.close()
        rs.close()

        System.out.println("Processed ${NumberFormat.getInstance().format(rowCount)} record(s) from ${import.table}")
    }

    private fun setupParameterList(columns: Int) : String {
        val result = StringBuffer()

        var first = true

        for (i in 0 until columns) {
            if (first) {
                first = false
            } else {
                result.append(", ")
            }

            result.append("?")
        }

        return result.toString()
    }

    private fun typeToTypeName(type: Int) : String {
        return when (type) {
            Types.ARRAY     -> "ARRAY"
            Types.BIGINT    -> "BIGINT"
            Types.BINARY    -> "BINARY"
            Types.BIT       -> "BIT"
            Types.BLOB      -> "BLOB"
            Types.CLOB      -> "CLOB"
            Types.BOOLEAN   -> "BIT"
            Types.CHAR      -> "CHAR"
            Types.DATE      -> "DATE"
            Types.DECIMAL   -> "DECIMAL"
            Types.DOUBLE    -> "DOUBLE"
            Types.FLOAT     -> "FLOAT"
            Types.INTEGER   -> "INT"
            Types.NCHAR     -> "NCHAR"
            Types.NUMERIC   -> "NUMERIC"
            Types.NVARCHAR  -> "NVARCHAR"
            Types.ROWID     -> "BIGINT"
            Types.SMALLINT  -> "SMALLINT"
            Types.SQLXML    -> "BLOB"
            Types.TIME      -> "TIME"
            Types.TIMESTAMP -> "TIMESTAMP"
            Types.TINYINT   -> "TINYINT"
            Types.VARBINARY -> "VARBINARY"
            Types.VARCHAR   -> "VARCHAR"
            else            -> "BLOB"
        }
    }

    private fun isSizable(type: String) : Boolean {
        val sizableTypes = arrayListOf("VARCHAR", "NUMERIC", "DECIMAL", "CHAR", "NCHAR", "NVARCHAR")

        return sizableTypes.contains(type)
    }

    private fun isNumeric(type: String) : Boolean {
        val numericTypes = arrayListOf("NUMERIC", "DECIMAL")

        return numericTypes.contains(type)
    }
}