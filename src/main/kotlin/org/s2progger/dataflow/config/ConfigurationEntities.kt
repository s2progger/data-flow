package org.s2progger.dataflow.config

data class PipelineConfiguration (
        val source: PipelineSourceConfig,
        val target: PipelineTargetConfig
)

data class PipelineTargetConfig (
        val exportBatchSize: Int?,
        val driver: String,
        val outputFolder: String?,
        val urlProtocol: String,
        val urlOptions: String,
        val testQuery: String?,
        val dialect: String?,
        val dataSourceProperties: List<DataSourceProperty>?,
        val sqlSetupCommand: String?,
        val username: String?,
        val password: String?
)

data class PipelineSourceConfig (
        val application: String,
        val rdms: DatabaseConnectionDetail
)

data class DatabaseConnectionDetail (
        val driver: String,
        val url: String,
        val username: String?,
        val password: String?,
        val schema: String?,
        val sqlSetupCommand: String?,
        val dataSourceProperties: List<DataSourceProperty>?,
        val testQuery: String?,
        val imports: List<DatabaseImport>,
        val targetPostScripts: List<PostRunScript>?
)

data class DatabaseImport (
        val table: String,
        val target: String?,
        val fetchSize: Int?,
        val query: String?,
        val targetPreTasks: List<DbTask>?,
        val targetPostTasks: List<DbTask>?
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
