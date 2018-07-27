package org.s2progger.dataflow

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.s2progger.dataflow.config.*
import java.io.File
import java.sql.*
import java.text.NumberFormat
import org.apache.commons.lang3.time.StopWatch
import org.s2progger.dataflow.dialects.DatabaseDialect
import org.s2progger.dataflow.dialects.GenericDialect
import org.s2progger.dataflow.dialects.MsSqlDialect
import org.s2progger.dataflow.dialects.OracleDialect
import org.slf4j.Logger
import kotlin.math.roundToInt

class DatabaseCopy(private val config: PipelineConfiguration, private val logger: Logger) {
    private val dialect: DatabaseDialect

    init {
        if (config.target.outputFolder != null) {
            File(config.target.outputFolder).mkdirs()
        }

        dialect = when (config.target.dialect?.toUpperCase()) {
            "ORACLE" -> OracleDialect()
            "MSSQL" -> MsSqlDialect()
            else -> GenericDialect()
        }
    }

    fun copyDatabase() {
        val exportUrl = when (config.target.outputFolder.isNullOrEmpty()) {
            true -> "${config.target.urlProtocol}${config.target.urlOptions}"
            false -> "${config.target.urlProtocol}${config.target.outputFolder}${config.source.application.toLowerCase().replace(" ", "_")}-import${config.target.urlOptions}"
        }

        val sourceConnectionConfig = HikariConfig()

        sourceConnectionConfig.poolName = "Source system connection pool"
        sourceConnectionConfig.driverClassName = config.source.rdms.driver
        sourceConnectionConfig.jdbcUrl = config.source.rdms.url
        sourceConnectionConfig.username = config.source.rdms.username
        sourceConnectionConfig.password = config.source.rdms.password

        if (config.source.rdms.testQuery != null)
            sourceConnectionConfig.connectionTestQuery = config.source.rdms.testQuery

        if (config.source.rdms.dataSourceProperties != null) {
            config.source.rdms.dataSourceProperties.forEach {
                sourceConnectionConfig.addDataSourceProperty(it.property, it.value)
            }
        }

        val targetConnectionConfig = HikariConfig()

        targetConnectionConfig.poolName = "Target database connection pool"
        targetConnectionConfig.driverClassName = config.target.driver
        targetConnectionConfig.jdbcUrl = exportUrl
        targetConnectionConfig.username = config.target.username
        targetConnectionConfig.password = config.target.password

        if (config.target.testQuery != null)
            targetConnectionConfig.connectionTestQuery = config.target.testQuery

        if (config.target.dataSourceProperties != null) {
            config.target.dataSourceProperties.forEach {
                targetConnectionConfig.addDataSourceProperty(it.property, it.value)
            }
        }

        val sourceDataSource = HikariDataSource(sourceConnectionConfig)
        val targetDataSource = HikariDataSource(targetConnectionConfig)

        if (config.target.sqlSetupCommand != null) {
            targetDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(config.target.sqlSetupCommand)
                }
            }
        }

        if (config.source.rdms.sqlSetupCommand != null) {
            sourceDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(config.source.rdms.sqlSetupCommand)
                }
            }
        }

        val targetMeta = getDbProductAndVersion(targetDataSource)

        logger.info("Target database product: ${targetMeta.productName}")
        logger.info("Target database version: ${targetMeta.version}")

        importApplication(sourceDataSource, targetDataSource, config.source.rdms.imports)

        if (config.source.rdms.targetPostScripts != null) {
            runPostScripts(config.source.rdms.targetPostScripts, targetDataSource)
        }

    }

    private fun getDbProductAndVersion(dataSource: HikariDataSource): DatabaseMetaData {
        dataSource.connection.use {
            return DatabaseMetaData(it.metaData.databaseProductName, it.metaData.databaseProductVersion)
        }
    }

    private data class DatabaseMetaData(val productName: String, val version: String)

    private fun importApplication(importDataSource: HikariDataSource, exportDataSource: HikariDataSource, importList: List<DatabaseImport>) {
        val importMeta = getDbProductAndVersion(importDataSource)

        logger.info("Source database product: ${importMeta.productName}")
        logger.info("Source database version: ${importMeta.version}")

        for (import in importList) {
            logger.info(import.table)

            if (import.targetPreTasks != null)
                runDbTasks(import.table, import.targetPreTasks, exportDataSource)

            prepareImportTable(import.table, import.target ?: import.table, importDataSource, exportDataSource)
            importTable(import, importDataSource, exportDataSource)

            if (import.targetPostTasks != null)
                runDbTasks(import.table, import.targetPostTasks, exportDataSource)
        }
    }

    private fun runPostScripts(scripts: List<PostRunScript>, dataSource: HikariDataSource) {
        for (script in scripts) {
            dataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    logger.info("Running script: ${script.label}...")

                    statement.executeUpdate(script.sql)

                    logger.info("${script.label} complete")
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun runDbTasks(table: String, tasks: List<DbTask>, dataSource: HikariDataSource) {
        logger.info("$table - TASK - Executing ${tasks.count()} task(s)")

        tasks.forEach { task ->
            try {
                dataSource.connection.use { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute(task.sql)
                    }
                }
            } catch (e: Exception) {
                logger.error("$table - TASK ERROR - Task exception: $e")
            }
        }
    }

    @Throws(Exception::class)
    private fun prepareImportTable(sourceTable: String, targetTable: String, importDataSource: HikariDataSource, exportDataSource: HikariDataSource) {
        try {
            // Check if the target table already exists
            exportDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    val tableDetectSql = "SELECT * FROM $targetTable WHERE 1 = 2"

                    statement.executeQuery(tableDetectSql)

                    logger.info("$sourceTable - TARGET EXISTS - Target already as $targetTable")
                }
            }
        } catch (e: Exception) {
            // Table doesn't exist, so create it
            val script = getTableCreateScript(sourceTable, targetTable, importDataSource)

            exportDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(script)

                    logger.info("$sourceTable - TARGET CREATED - Target created as $targetTable")
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun getTableCreateScript(sourceTable: String, targetTable: String, dataSource: HikariDataSource) : String {
        val script = StringBuffer()

        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                val tableDetectSql = "SELECT * FROM $sourceTable WHERE 1 = 2"
                val rs = statement.executeQuery(tableDetectSql)
                val meta = rs.metaData

                script.append("CREATE TABLE $targetTable ( ")

                var first = true

                for (i in 1..meta.columnCount) {
                    if (first) {
                        first = false
                    } else {
                        script.append(", ")
                    }

                    val name = meta.getColumnName(i)
                    val type = dialect.typeToTypeName(meta.getColumnType(i))
                    val size = meta.getPrecision(i)
                    val scale = meta.getScale(i)

                    val nullable = if (meta.isNullable(i) == ResultSetMetaData.columnNoNulls) "NOT NULL" else ""

                    if (dialect.isSizable(type) && dialect.isNumeric(type)) {
                        if (size == 0)
                            script.append("$name $type (${dialect.defaultMaxNumberSize()}) $nullable")
                        else
                            script.append("$name $type ($size, $scale) $nullable")
                    } else if(dialect.isSizable(type)) {
                        val targetSize = if (size == 0) dialect.defaultMaxDataSize() else size.toString()

                        script.append("$name $type ($targetSize) $nullable")
                    } else {
                        script.append("$name $type $nullable")
                    }
                }

                script.append(")")

                rs.close()
            }
        }

        return script.toString()
    }

    @Throws(Exception::class)
    private fun importTable(import: DatabaseImport, importDataSource: HikariDataSource, exportDataSource: HikariDataSource) {
        val insertBatchSize = config.target.exportBatchSize ?: 10000
        val logBatchSize = config.global?.logging?.logBatchSize ?: insertBatchSize
        val columnDetectSql = "SELECT * FROM ${import.table} WHERE 1 = 2"
        val selectSql = import.query ?: "SELECT * FROM ${import.table}"
        val insertSql = StringBuffer()
        val columnTypes = ArrayList<Int>()

        importDataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                val metaRs = statement.executeQuery(columnDetectSql)
                val meta = metaRs.metaData

                for (i in 1..meta.columnCount) {
                    columnTypes.add(meta.getColumnType(i))
                }

                insertSql.append("INSERT INTO ${import.target ?: import.table} VALUES (${setupParameterList(meta.columnCount)})")

                metaRs.close()
            }
        }

        importDataSource.connection.use { importConnection ->
            // Attempt to use a forward moving cursor for result sets in order to cut down on memory usage when fetching millions of rows
            importConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { importStatement ->
                if (import.fetchSize != null)
                    importStatement.fetchSize = import.fetchSize

                logger.info("${import.table} - SELECT - Executing select statement")

                val rs = importStatement.executeQuery(selectSql)

                logger.info("${import.table} - RECEIVED - Result set received - copying to target")

                exportDataSource.connection.use { exportConnection ->
                    exportConnection.autoCommit = false

                    exportConnection.prepareStatement(insertSql.toString()).use { exportStatement ->
                        var rowCount = 0
                        val timer = StopWatch()

                        timer.start()

                        while (rs.next()) {
                            rowCount++

                            for (i in 1..columnTypes.count()) {
                                dialect.setPsValueFromRs(columnTypes[i - 1], rs, i, exportStatement, i)
                            }

                            exportStatement.addBatch()

                            if (rowCount % insertBatchSize == 0) {
                                exportStatement.executeBatch()
                                exportStatement.clearParameters()

                                exportConnection.commit()
                            }

                            if (rowCount % logBatchSize == 0) {
                                val speed: Double = rowCount.toDouble() / (timer.time.toDouble() / 1000.toDouble())

                                logger.info("${import.table} - COPIED - ${NumberFormat.getInstance().format(rowCount)} rows(s) copied (Avg speed: ${speed.roundToInt()} rows/s)")
                            }
                        }

                        exportStatement.executeBatch()
                        exportConnection.commit()

                        timer.stop()
                        rs.close()

                        val speed: Double = rowCount.toDouble() / (timer.time.toDouble() / 1000.toDouble())

                        logger.info("${import.table} - COMPLETED - ${NumberFormat.getInstance().format(rowCount)} rows(s) copied (Avg speed: ${speed.roundToInt()} rows/s)")
                    }
                }
            }
        }
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
}