package org.s2progger.dataflow.dialects

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

open class GenericDialect : DatabaseDialect {
    override fun typeToTypeName(type: Int): String {
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
            Types.LONGVARBINARY -> "BLOB"
            Types.LONGVARCHAR   -> "CLOB"
            else            -> "BLOB"
        }
    }

    override fun setPsValueFromRs(type: Int, resultSet: ResultSet, resultSetIndex: Int, statement: PreparedStatement, statementIndex: Int) {
        when (type) {
            Types.ARRAY -> statement.setArray(statementIndex, resultSet.getArray(resultSetIndex))
            Types.BIGINT -> statement.setLong(statementIndex, resultSet.getLong(resultSetIndex))
            Types.BINARY -> statement.setBinaryStream(statementIndex, resultSet.getBinaryStream(resultSetIndex))
            Types.BIT -> statement.setBoolean(statementIndex, resultSet.getBoolean(resultSetIndex))
            Types.BLOB -> statement.setBlob(statementIndex, resultSet.getBlob(resultSetIndex))
            Types.CLOB -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            Types.BOOLEAN -> statement.setBoolean(statementIndex, resultSet.getBoolean(resultSetIndex))
            Types.CHAR -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            Types.DATE -> statement.setDate(statementIndex, resultSet.getDate(resultSetIndex))
            Types.DECIMAL -> statement.setBigDecimal(statementIndex, resultSet.getBigDecimal(resultSetIndex))
            Types.DOUBLE -> statement.setDouble(statementIndex, resultSet.getDouble(resultSetIndex))
            Types.FLOAT -> statement.setFloat(statementIndex, resultSet.getFloat(resultSetIndex))
            Types.INTEGER -> statement.setInt(statementIndex, resultSet.getInt(resultSetIndex))
            Types.NCHAR -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            Types.NUMERIC -> statement.setBigDecimal(statementIndex, resultSet.getBigDecimal(resultSetIndex))
            Types.NVARCHAR -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            Types.ROWID -> statement.setLong(statementIndex, resultSet.getLong(resultSetIndex))
            Types.SMALLINT -> statement.setShort(statementIndex, resultSet.getShort(resultSetIndex))
            Types.SQLXML -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            Types.TIME -> statement.setTime(statementIndex, resultSet.getTime(resultSetIndex))
            Types.TIMESTAMP -> statement.setDate(statementIndex, resultSet.getDate(resultSetIndex))
            Types.TINYINT -> statement.setByte(statementIndex, resultSet.getByte(resultSetIndex))
            Types.VARBINARY -> statement.setBytes(statementIndex, resultSet.getBytes(resultSetIndex))
            Types.VARCHAR -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            Types.LONGVARBINARY -> statement.setBytes(statementIndex, resultSet.getBytes(resultSetIndex))
            Types.LONGVARCHAR -> statement.setString(statementIndex, resultSet.getString(resultSetIndex))
            else -> statement.setBlob(statementIndex, resultSet.getBlob(resultSetIndex))
        }
    }
    
    override fun isSizable(type: String): Boolean {
        val sizableTypes = arrayListOf("VARCHAR2", "VARCHAR", "NUMERIC", "NUMBER", "DECIMAL", "CHAR", "NCHAR", "NVARCHAR", "NVARCHAR2", "VARBINARY", "BINARY")

        return sizableTypes.contains(type)
    }

    override fun isNumeric(type: String): Boolean {
        val numericTypes = arrayListOf("NUMERIC", "DECIMAL", "NUMBER")

        return numericTypes.contains(type)
    }
}
