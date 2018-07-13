package org.s2progger.dataflow

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import org.s2progger.dataflow.config.DatabaseConnectionDetail
import org.s2progger.dataflow.config.DatabaseImport
import org.s2progger.dataflow.config.ExportDbConfiguration
import org.s2progger.dataflow.config.PostRunScript
import java.io.File
import java.sql.*
import java.text.NumberFormat

class DatabaseCopy(private val exportConfig: ExportDbConfiguration) {
    private val logger = KotlinLogging.logger {}

    init {
        if (exportConfig.outputFolder != null) {
            File(exportConfig.outputFolder).mkdirs()
        }
    }

    fun copyDatabase(dbName: String, details: DatabaseConnectionDetail) {
        val exportUrl = when (exportConfig.outputFolder.isNullOrEmpty()) {
            true -> "${exportConfig.urlProtocol}${exportConfig.urlOptions}"
            false -> "${exportConfig.urlProtocol}${exportConfig.outputFolder}${dbName.toLowerCase().replace(" ", "_")}-import${exportConfig.urlOptions}"
        }

        val importConnectionConfig = HikariConfig()

        importConnectionConfig.poolName = "Import connection pool"
        importConnectionConfig.driverClassName = details.driver
        importConnectionConfig.jdbcUrl = details.url
        importConnectionConfig.username = details.username
        importConnectionConfig.password = details.password

        if (details.testQuery != null)
            importConnectionConfig.connectionTestQuery = details.testQuery

        if (details.dataSourceProperties != null) {
            details.dataSourceProperties.forEach {
                importConnectionConfig.addDataSourceProperty(it.property, it.value)
            }
        }

        val exportConnectionConfig = HikariConfig()

        exportConnectionConfig.poolName = "Export connection pool"
        exportConnectionConfig.driverClassName = exportConfig.driver
        exportConnectionConfig.jdbcUrl = exportUrl
        exportConnectionConfig.username = exportConfig.username
        exportConnectionConfig.password = exportConfig.password

        if (exportConfig.testQuery != null)
            exportConnectionConfig.connectionTestQuery = exportConfig.testQuery

        if (exportConfig.dataSourceProperties != null) {
            exportConfig.dataSourceProperties.forEach {
                exportConnectionConfig.addDataSourceProperty(it.property, it.value)
            }
        }

        val importDataSource = HikariDataSource(importConnectionConfig)
        val exportDataSource = HikariDataSource(exportConnectionConfig)

        if (details.sqlSetupCommands != null) {
            importDataSource.connection.use { connection ->
                connection.createStatement().use {statement ->
                    statement.execute(details.sqlSetupCommands)
                }
            }
        }

        if (exportConfig.sqlSetupCommands != null) {
            exportDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(exportConfig.sqlSetupCommands)
                }
            }
        }

        importApplication(importDataSource, exportDataSource, details.imports)

        if (details.postScripts != null) {
            runPostScripts(details.postScripts, exportDataSource)
        }

    }

    private fun getDbProductAndVersion(dataSource: HikariDataSource): DatabaseMetaData {
        dataSource.connection.use {
            return DatabaseMetaData(it.metaData.databaseProductName, it.metaData.databaseProductVersion)
        }
    }

    private fun importApplication(importDataSource: HikariDataSource, exportDataSource: HikariDataSource, importList: List<DatabaseImport>) {
        val importMeta = getDbProductAndVersion(importDataSource)

        logger.info("Database product: ${importMeta.productName}")
        logger.info("Database version: ${importMeta.version}")

        for (import in importList) {
            logger.info("Importing ${import.table}...")

            prepareImportTable(import.table, import.target ?: import.table, importDataSource, exportDataSource)

            importTable(import, importDataSource, exportDataSource)
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
    private fun prepareImportTable(sourceTable: String, targetTable: String, importDataSource: HikariDataSource, exportDataSource: HikariDataSource) {
        try {
            // Check if the target table already exists
            exportDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    val tableDetectSql = "SELECT * FROM $sourceTable WHERE 1 = 2"

                    statement.executeQuery(tableDetectSql)

                    logger.info("Target table [$sourceTable] already exists and will be used")
                }
            }
        } catch (e: Exception) {
            // Table doesn't exist, so create it
            val script = getTableCreateScript(sourceTable, targetTable, importDataSource)

            exportDataSource.connection.use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(script)
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun getTableCreateScript(sourceTable: String, tartgetTable: String, dataSource: HikariDataSource) : String {
        val script = StringBuffer()

        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                val tableDetectSql = "SELECT * FROM $sourceTable WHERE 1 = 2"
                val rs = statement.executeQuery(tableDetectSql)
                val meta = rs.metaData

                script.append("CREATE TABLE $tartgetTable ( ")

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

                    if (isSizable(type) && isNumeric(type) && precision >= 0) {
                        script.append("$name $type ($size, $precision) $nullable")
                    } else if(isSizable(type) && precision >= 0) {
                        // If this is a sizable type and the size is 0, the JDBC driver probably isn't implemented correctly
                        // so just the target column the max size (this will need to be reworked to be more DB agnostic)
                        val colSize = if (size == 0) "MAX" else size.toString()

                        script.append("$name $type ($colSize) $nullable")
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
        val insertBatchSize = exportConfig.exportBatchSize ?: 10000
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

                if (import.preQuery != null)
                    importStatement.executeUpdate(import.preQuery)

                val rs = importStatement.executeQuery(selectSql)

                exportDataSource.connection.use { exportConnection ->
                    exportConnection.autoCommit = false

                    exportConnection.prepareStatement(insertSql.toString()).use { exportStatement ->
                        var rowCount = 0

                        while (rs.next()) {
                            rowCount++

                            for (i in 1..columnTypes.count()) {
                                when (columnTypes[i - 1]) {
                                    Types.ARRAY -> exportStatement.setArray(i, rs.getArray(i))
                                    Types.BIGINT -> exportStatement.setLong(i, rs.getLong(i))
                                    Types.BINARY -> exportStatement.setBinaryStream(i, rs.getBinaryStream(i))
                                    Types.BIT -> exportStatement.setBoolean(i, rs.getBoolean(i))
                                    Types.BLOB -> exportStatement.setBlob(i, rs.getBlob(i))
                                    Types.CLOB -> exportStatement.setString(i, rs.getString(i))
                                    Types.BOOLEAN -> exportStatement.setBoolean(i, rs.getBoolean(i))
                                    Types.CHAR -> exportStatement.setString(i, rs.getString(i))
                                    Types.DATE -> exportStatement.setDate(i, rs.getDate(i))
                                    Types.DECIMAL -> exportStatement.setBigDecimal(i, rs.getBigDecimal(i))
                                    Types.DOUBLE -> exportStatement.setDouble(i, rs.getDouble(i))
                                    Types.FLOAT -> exportStatement.setFloat(i, rs.getFloat(i))
                                    Types.INTEGER -> exportStatement.setInt(i, rs.getInt(i))
                                    Types.NCHAR -> exportStatement.setString(i, rs.getString(i))
                                    Types.NUMERIC -> exportStatement.setBigDecimal(i, rs.getBigDecimal(i))
                                    Types.NVARCHAR -> exportStatement.setString(i, rs.getString(i))
                                    Types.ROWID -> exportStatement.setLong(i, rs.getLong(i))
                                    Types.SMALLINT -> exportStatement.setShort(i, rs.getShort(i))
                                    Types.SQLXML -> exportStatement.setString(i, rs.getString(i))
                                    Types.TIME -> exportStatement.setTime(i, rs.getTime(i))
                                    Types.TIMESTAMP -> exportStatement.setDate(i, rs.getDate(i))
                                    Types.TINYINT -> exportStatement.setByte(i, rs.getByte(i))
                                    Types.VARBINARY -> exportStatement.setBytes(i, rs.getBytes(i))
                                    Types.VARCHAR -> exportStatement.setString(i, rs.getString(i))
                                    Types.LONGVARBINARY -> exportStatement.setBytes(i, rs.getBytes(i))
                                    Types.LONGVARCHAR -> exportStatement.setString(i, rs.getString(i))
                                    else -> exportStatement.setBlob(i, rs.getBlob(i))
                                }
                            }

                            exportStatement.addBatch()

                            if (rowCount % insertBatchSize == 0) {
                                exportStatement.executeBatch()
                                exportStatement.clearParameters()

                                exportConnection.commit()

                                logger.info("Exported ${NumberFormat.getInstance().format(rowCount)} records so far...")
                            }
                        }

                        exportStatement.executeBatch()
                        exportConnection.commit()

                        rs.close()

                        logger.info("Processed ${NumberFormat.getInstance().format(rowCount)} record(s) from ${import.table}")
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
            Types.TIMESTAMP -> "DATETIME"
            Types.TINYINT   -> "TINYINT"
            Types.VARBINARY -> "VARBINARY"
            Types.VARCHAR   -> "VARCHAR"
            Types.LONGVARBINARY -> "VARBINARY(MAX)"
            Types.LONGVARCHAR   -> "VARCHAR(MAX)"
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

    private data class DatabaseMetaData(val productName: String, val version: String)
}