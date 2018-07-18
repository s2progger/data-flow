package org.s2progger.dataflow.dialects

import java.sql.Types

class OracleDialect : GenericDialect() {
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
            Types.LONGVARBINARY -> "VARBINARY(MAX)"
            Types.LONGVARCHAR   -> "VARCHAR(MAX)"
            else            -> "BLOB"
        }
    }
}
