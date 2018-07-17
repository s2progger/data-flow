package org.s2progger.dataflow.config

data class ExportDbConfiguration (
        val exportBatchSize: Int?,
        val driver: String,
        val outputFolder: String?,
        val urlProtocol: String,
        val urlOptions: String,
        val testQuery: String?,
        val dataSourceProperties: List<DataSourceProperty>?,
        val sqlSetupCommands: String?,
        val username: String?,
        val password: String?
)

data class ImportConfig (
        val knownDatabases: List<KnownDatabase>
)

data class KnownDatabase (
        val application: String,
        val connectionDetails: DatabaseConnectionDetail
)

data class DatabaseConnectionDetail (
        val driver: String,
        val h2CompatibilityMode: String?,
        val url: String,
        val username: String?,
        val password: String?,
        val schema: String?,
        val sqlSetupCommands: String?,
        val dataSourceProperties: List<DataSourceProperty>?,
        val testQuery: String?,
        val imports: List<DatabaseImport>,
        val postScripts: List<PostRunScript>?
)

data class DatabaseImport (
        val table: String,
        val target: String?,
        val fetchSize: Int?,
        val query: String?,
        val preTasks: List<DbTask>?,
        val postTasks: List<DbTask>?
)

data class DbTask (
        val sql: String
)

data class PostRunScript (
        val label: String,
        val sql: String
)

data class DataSourceProperty (
        val property: String,
        val value: String
)
