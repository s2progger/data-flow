package com.inferwerx.dbextraction

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
        val imports: List<DatabaseImport>,
        val postScripts: List<PostRunScript>?
)

data class DatabaseImport (
        val table: String,
        val query: String?
)

data class PostRunScript (
        val label: String,
        val sql: String
)