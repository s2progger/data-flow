package com.inferwerx.dbextraction

data class ExportDbConfiguration (
        val driver: String,
        val outputFolder: String?,
        val urlProtocol: String,
        val urlOptions: String,
        val sqlSetupCommands: String?,
        val username: String?,
        val password: String?
)