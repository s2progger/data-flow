package org.s2progger.dataflow.dialects

import java.sql.Types

/* https://docs.oracle.com/cd/B28359_01/java.111/b31226/datamap.htm */
class OracleDialect : GenericDialect() {
    override fun typeToTypeName(type: Int): String {
        return when (type) {
            Types.ARRAY     -> "VARRAY"
            Types.BIGINT    -> "BIGINT"
            Types.BINARY    -> "BLOB"
            Types.BIT       -> "BIT"
            Types.BLOB      -> "BLOB"
            Types.CLOB      -> "CLOB"
            Types.BOOLEAN   -> "BOOLEAN"
            Types.CHAR      -> "CHAR"
            Types.DATE      -> "DATE"
            Types.DECIMAL   -> "DECIMAL"
            Types.DOUBLE    -> "DOUBLE PRECISION"
            Types.FLOAT     -> "DOUBLE PRECISION"
            Types.INTEGER   -> "INTEGER"
            Types.NCHAR     -> "NCHAR"
            Types.NUMERIC   -> "NUMBER"
            Types.NVARCHAR  -> "NVARCHAR2"
            Types.ROWID     -> "BIGINT"
            Types.SMALLINT  -> "SMALLINT"
            Types.SQLXML    -> "XML"
            Types.TIME      -> "DATE"
            Types.TIMESTAMP -> "DATE"
            Types.TINYINT   -> "TINYINT"
            Types.VARBINARY -> "VARCHAR FOR BIT DATA"
            Types.VARCHAR   -> "VARCHAR2"
            Types.LONGVARBINARY -> "BLOB"
            Types.LONGVARCHAR   -> "CLOB"
            else            -> "BLOB"
        }
    }
}
