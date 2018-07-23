package org.s2progger.dataflow.dialects

import java.sql.PreparedStatement
import java.sql.ResultSet

interface DatabaseDialect {
    fun typeToTypeName(type: Int): String
    fun isSizable(type: String): Boolean
    fun isNumeric(type: String): Boolean
    fun defaultMaxNumberSize(): String
    fun defaultMaxDataSize(): String
    fun setPsValueFromRs(type: Int, resultSet: ResultSet, resultSetIndex: Int, statement: PreparedStatement, statementIndex: Int)
}
