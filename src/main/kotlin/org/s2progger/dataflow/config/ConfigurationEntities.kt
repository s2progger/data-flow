package org.s2progger.dataflow

data class ExportDbConfiguration (
        val exportBatchSize: Int?,
        val driver: String,
        val outputFolder: String?,
        val urlProtocol: String,
        val urlOptions: String,
        val sqlSetupCommands: String?,
        val username: String?,
        val password: String?
)