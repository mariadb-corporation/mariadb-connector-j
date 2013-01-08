/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc;

import java.sql.*;


public class MySQLDatabaseMetaData implements DatabaseMetaData {
    private String version;
    private String url;
    private String username;
    private Connection connection;
    private String databaseProductName = "MySQL";

    protected final String dataTypeClause = " CASE data_type" +
                        " when 'bit' then "         + Types.BIT +
                        " when 'tinyblob' then "    + Types.LONGVARBINARY +
                        " when 'mediumblob' then "  + Types.LONGVARBINARY +
                        " when 'longblob' then "    + Types.LONGVARBINARY +
                        " when 'blob' then "        + Types.LONGVARBINARY +
                        " when 'tinytext' then "    + Types.LONGVARCHAR +
                        " when 'mediumtext' then "  + Types.LONGVARCHAR +
                        " when 'longtext' then "    + Types.LONGVARCHAR +
                        " when 'text' then "        + Types.LONGVARCHAR +
                        " when 'date' then "        + Types.DATE +
                        " when 'datetime' then "    + Types.TIMESTAMP +
                        " when 'decimal' then "     + Types.DECIMAL +
                        " when 'double' then "      + Types.DOUBLE +
                        " when 'enum' then "        + Types.VARCHAR +
                        " when 'float' then "       + Types.FLOAT +
                        " when 'int' then "         + Types.INTEGER +
                        " when 'bigint' then "      + Types.BIGINT +
                        " when 'mediumint' then "   + Types.INTEGER +
                        " when 'null' then "        + Types.NULL +
                        " when 'set' then "         + Types.VARCHAR +
                        " when 'smallint' then "    + Types.SMALLINT +
                        " when 'varchar' then "     + Types.VARCHAR +
                        " when 'varbinary' then "   + Types.VARBINARY +
                        " when 'char' then "        + Types.CHAR +
                        " when 'binary' then "      + Types.BINARY +
                        " when 'time' then "        + Types.TIME +
                        " when 'timestamp' then "   + Types.TIMESTAMP +
                        " when 'tinyint' then "     + Types.TINYINT +
                        " when 'year' then "        + Types.SMALLINT +
                        " END ";

    public MySQLDatabaseMetaData(Connection connection, String user, String url) {
        this.connection = connection;
        this.username = user;
        this.url = url;
    }


    public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
        final Connection conn = getConnection();
        String query = "SELECT NULL TABLE_CAT, " +
                "table_schema TABLE_SCHEM, " +
                "table_name, " +
                "column_name, " +
                "ordinal_position KEY_SEQ," +
                "null pk_name " +
                "FROM information_schema.columns " +
                "WHERE table_name='" + table + "' AND column_key='pri'";

        if (schema != null && (((MySQLConnection)conn).noSchemaPattern == false)) {
            query += " AND table_schema = '" + schema + "'";
        }
        query += " ORDER BY column_name";
        final Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }
   /**
     * Maps standard table types to mysql ones - helper since table type is never "table" in mysql, it is "base table"
     * @param tableType the table type defined by user
     * @return the internal table type.
     */
    private String mapTableTypes(String tableType) {
        if(tableType.equals("TABLE")) {
            return "BASE TABLE";
        }
        return tableType;
    }


    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, final String[] types) throws SQLException {

        if (tableNamePattern == null || tableNamePattern.equals(""))
            tableNamePattern = "%";

        String query = "SELECT table_catalog table_cat, "
                        + "table_schema table_schem, "
                        + "table_name, "
                        + "table_type, "
                        + "table_comment as remarks,"
                        + "null as type_cat, "
                        + "null as type_schem,"
                        + "null as type_name, "
                        + "null as self_referencing_col_name,"
                        + "null as ref_generation "
                        + "FROM information_schema.tables "
                        + "WHERE table_name LIKE '"+(tableNamePattern == null?"%":tableNamePattern)+"'"
                        + getSchemaPattern(schemaPattern);

        if(types != null) {
            query += " AND table_type in (";
            boolean first = true;
            for(String s : types) {
                String mappedType = mapTableTypes(s);
                if(!first) {
                    query += ",";
                }
                first = false;
                query += "'"+mappedType+"'";
            }
            query += ")";
        }
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }

        public ResultSet getColumns( String catalog,  String schemaPattern,  String tableNamePattern,  String columnNamePattern)
            throws SQLException {

        if (tableNamePattern == null || tableNamePattern.equals(""))
            tableNamePattern = "%";

        if (columnNamePattern == null || columnNamePattern.equals(""))
            columnNamePattern = "%";

        if (schemaPattern == null || schemaPattern.equals(""))
            schemaPattern = "%";

        String query = "     SELECT null as table_cat," +
                "            table_schema as table_schem," +
                "            table_name," +
                "            column_name," +
                dataTypeClause + " data_type," +
                "            column_type type_name," +
                "            character_maximum_length column_size," +
                "            0 buffer_length," +
                "            numeric_precision decimal_digits," +
                "            numeric_scale num_prec_radix," +
                "            if(is_nullable='yes',1,0) nullable," +
                "            column_comment remarks," +
                "            column_default column_def," +
                "            0 sql_data," +
                "            0 sql_datetime_sub," +
                "            character_octet_length char_octet_length," +
                "            ordinal_position," +
                "            is_nullable," +
                "            null scope_catalog," +
                "            null scope_schema," +
                "            null scope_table," +
                "            null source_data_type," +
                "            '' is_autoincrement" +
                "    FROM information_schema.columns " +
                "WHERE ";
        if (((MySQLConnection)getConnection()).noSchemaPattern == false)
        {
            query = query + "table_schema LIKE '" + schemaPattern + "' AND ";
        }
        query = query + "table_name LIKE '" + tableNamePattern  + "'" +
                " AND column_name LIKE '" +  columnNamePattern + "'" +
                " ORDER BY table_cat, table_schem, table_name, ordinal_position";
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }

    public ResultSet getExportedKeys(final String catalog, final String schema, final String table) throws SQLException {
        final Connection conn = getConnection();
        final String query = "SELECT null PKTABLE_CAT, \n" +
                "kcu.referenced_table_schema PKTABLE_SCHEM, \n" +
                "kcu.referenced_table_name PKTABLE_NAME, \n" +
                "kcu.referenced_column_name PKCOLUMN_NAME, \n" +
                "null FKTABLE_CAT, \n" +
                "kcu.table_schema FKTABLE_SCHEM, \n" +
                "kcu.table_name FKTABLE_NAME, \n" +
                "kcu.column_name FKCOLUMN_NAME, \n" +
                "kcu.position_in_unique_constraint KEY_SEQ,\n" +
                "CASE update_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "CASE delete_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "rc.constraint_name FK_NAME,\n" +
                "null PK_NAME,\n" +
                "6 DEFERRABILITY\n" +
                "FROM information_schema.key_column_usage kcu\n" +
                "INNER JOIN information_schema.referential_constraints rc\n" +
                "ON kcu.constraint_schema=rc.constraint_schema\n" +
                "AND kcu.constraint_name=rc.constraint_name\n" +
                "WHERE " +
                (schema != null && (((MySQLConnection)conn).noSchemaPattern == false) ? "kcu.referenced_table_schema='" + schema + "' AND " : "") +
                "kcu.referenced_table_name='" +
                table +
                "'" +
                " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        final Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(query);
    }
    public ResultSet getImportedKeys(final String catalog, final String schema, final String table) throws SQLException {
        Connection conn = getConnection();
        String query = "SELECT null PKTABLE_CAT, \n" +
                "kcu.referenced_table_schema PKTABLE_SCHEM, \n" +
                "kcu.referenced_table_name PKTABLE_NAME, \n" +
                "kcu.referenced_column_name PKCOLUMN_NAME, \n" +
                "null FKTABLE_CAT, \n" +
                "kcu.table_schema FKTABLE_SCHEM, \n" +
                "kcu.table_name FKTABLE_NAME, \n" +
                "kcu.column_name FKCOLUMN_NAME, \n" +
                "kcu.position_in_unique_constraint KEY_SEQ,\n" +
                "CASE update_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "CASE delete_rule \n" +
                "   WHEN 'RESTRICT' THEN 1\n" +
                "   WHEN 'NO ACTION' THEN 3\n" +
                "   WHEN 'CASCADE' THEN 0\n" +
                "   WHEN 'SET NULL' THEN 2\n" +
                "   WHEN 'SET DEFAULT' THEN 4\n" +
                "END UPDATE_RULE,\n" +
                "rc.constraint_name FK_NAME,\n" +
                "null PK_NAME,\n" +
                "6 DEFERRABILITY\n" +
                "FROM information_schema.key_column_usage kcu\n" +
                "INNER JOIN information_schema.referential_constraints rc\n" +
                "ON kcu.constraint_schema=rc.constraint_schema\n" +
                "AND kcu.constraint_name=rc.constraint_name\n" +
                "WHERE " +
                (schema != null && (((MySQLConnection)conn).noSchemaPattern == false) ? "kcu.table_schema='" + schema + "' AND " : "") +
                "kcu.table_name='" +
                table +
                "'" +
                " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";

        Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }

    public ResultSet getBestRowIdentifier(final String catalog, final String schema, final String table, final int scope, final boolean nullable)
            throws SQLException {
        String query = "SELECT " + DatabaseMetaData.bestRowSession + " scope," +
                "column_name," +
                dataTypeClause + " data_type," +
                "data_type type_name," +
                "if(numeric_precision is null, character_maximum_length, numeric_precision) column_size," +
                "0 buffer_length," +
                "numeric_scale decimal_digits," +
                DatabaseMetaData.bestRowNotPseudo + " pseudo_column" +
                " FROM information_schema.columns" +
                " WHERE column_key in('PRI', 'MUL', 'UNI') ";
        Connection conn = getConnection();
        if (((MySQLConnection)conn).noSchemaPattern == false)
        {
                query = query + " AND table_schema like " + (schema != null ? "'%'" : "'" + schema + "'");
        }
        query = query + " AND table_name='" + table + "' ORDER BY scope";
        final Statement stmt = conn.createStatement();
        return stmt.executeQuery(query);
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    public ResultSet getPseudoColumns(String arg0, String arg1, String arg2,
            String arg3) throws SQLException {
        return MySQLResultSet.EMPTY;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    public String getURL() throws SQLException {
        return url;
    }

    public String getUserName() throws SQLException {
        return username;
    }


    public boolean isReadOnly() throws SQLException {
        return false;
    }


    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }


    public boolean nullsAreSortedLow() throws SQLException {
        return !nullsAreSortedHigh();
    }


    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return !nullsAreSortedAtStart();
    }


    public String getDatabaseProductName() throws SQLException {
        return databaseProductName;
    }


    public String getDatabaseProductVersion() throws SQLException {
        if (version == null) {
            ResultSet rs = connection.createStatement().executeQuery("select version()");
            rs.next();
            version = rs.getString(1);
        }
        return version;
    }


    public String getDriverName() throws SQLException {
        return "mariadb-jdbc"; // TODO: get from constants file
    }

    public String getDriverVersion() throws SQLException {
        return String.format("%d.%d",getDriverMajorVersion(),getDriverMinorVersion());
    }


    public int getDriverMajorVersion() {
        return 1;
    }

    public int getDriverMinorVersion() {
        return 2;
    }


    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return !storesUpperCaseIdentifiers();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return !storesMixedCaseQuotedIdentifiers();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    public String getSQLKeywords() throws SQLException {
        return
        "ACCESSIBLE,"+
        "ANALYZE,"+
        "ASENSITIVE,"+
        "BEFORE,"+
        "BIGINT,"+
        "BINARY,"+
        "BLOB,"+
        "CALL,"+
        "CHANGE,"+
        "CONDITION,"+
        "DATABASE,"+
        "DATABASES,"+
        "DAY_HOUR,"+
        "DAY_MICROSECOND,"+
        "DAY_MINUTE,"+
        "DAY_SECOND,"+
        "DELAYED,"+
        "DETERMINISTIC,"+
        "DISTINCTROW,"+
        "DIV,"+
        "DUAL,"+
        "EACH,"+
        "ELSEIF,"+
        "ENCLOSED,"+
        "ESCAPED,"+
        "EXIT,"+
        "EXPLAIN,"+
        "FLOAT4,"+
        "FLOAT8,"+
        "FORCE,"+
        "FULLTEXT,"+
        "HIGH_PRIORITY,"+
        "HOUR_MICROSECOND,"+
        "HOUR_MINUTE,"+
        "HOUR_SECOND,"+
        "IF,"+
        "IGNORE,"+
        "INFILE,"+
        "INOUT,"+
        "INT1,"+
        "INT2,"+
        "INT3,"+
        "INT4,"+
        "INT8,"+
        "ITERATE,"+
        "KEY," +
        "KEYS,"+
        "KILL,"+
        "LEAVE,"+
        "LIMIT,"+
        "LINEAR,"+
        "LINES,"+
        "LOAD,"+
        "LOCALTIME,"+
        "LOCALTIMESTAMP,"+
        "LOCK,"+
        "LONG,"+
        "LONGBLOB,"+
        "LONGTEXT,"+
        "LOOP,"+
        "LOW_PRIORITY,"+
        "MEDIUMBLOB,"+
        "MEDIUMINT,"+
        "MEDIUMTEXT,"+
        "MIDDLEINT,"+
        "MINUTE_MICROSECOND,"+
        "MINUTE_SECOND,"+
        "MOD,"+
        "MODIFIES,"+
        "NO_WRITE_TO_BINLOG,"+
        "OPTIMIZE,"+
        "OPTIONALLY,"+
        "OUT,"+
        "OUTFILE,"+
        "PURGE,"+
        "RANGE,"+
        "READS,"+
        "READ_ONLY,"+
        "READ_WRITE,"+
        "REGEXP,"+
        "RELEASE,"+
        "RENAME,"+
        "REPEAT,"+
        "REPLACE,"+
        "REQUIRE,"+
        "RETURN,"+
        "RLIKE,"+
        "SCHEMAS,"+
        "SECOND_MICROSECOND,"+
        "SENSITIVE,"+
        "SEPARATOR,"+
        "SHOW,"+
        "SPATIAL,"+
        "SPECIFIC,"+
        "SQLEXCEPTION,"+
        "SQL_BIG_RESULT,"+
        "SQL_CALC_FOUND_ROWS,"+
        "SQL_SMALL_RESULT,"+
        "SSL,"+
        "STARTING,"+
        "STRAIGHT_JOIN,"+
        "TERMINATED,"+
        "TINYBLOB,"+
        "TINYINT,"+
        "TINYTEXT,"+
        "TRIGGER,"+
        "UNDO,"+
        "UNLOCK,"+
        "UNSIGNED,"+
        "USE,"+
        "UTC_DATE,"+
        "UTC_TIME,"+
        "UTC_TIMESTAMP,"+
        "VARBINARY,"+
        "VARCHARACTER,"+
        "WHILE,"+
        "X509,"+
        "XOR,"+
        "YEAR_MONTH,"+
        "ZEROFILL,"+
        "GENERAL,"+
        "IGNORE_SERVER_IDS,"+
        "MASTER_HEARTBEAT_PERIOD,"+
        "MAXVALUE,"+
        "RESIGNAL,"+
        "SIGNAL,"+
        "SLOW";
    }

    public String getNumericFunctions() throws SQLException {
        return ""; //TODO : fix
    }


    public String getStringFunctions() throws SQLException {
        return ""; //TODO: fix
    }

    public String getSystemFunctions() throws SQLException {
        return "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION";
    }


    public String getTimeDateFunctions() throws SQLException {
        return ""; //TODO : fix
    }

    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    public String getExtraNameCharacters() throws SQLException {
        return "#@";
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    public boolean supportsConvert() throws SQLException {
        return false; //TODO: fix
    }

    public boolean supportsConvert(final int fromType, final int toType)
            throws SQLException {
                return false; // TODO: fix
            }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false; //TODO: verify
    }

    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    public String getSchemaTerm() throws SQLException {
        return "schema";
    }


    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    public String getCatalogTerm() throws SQLException {
        return "database";
    }


    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }


    public String getCatalogSeparator() throws SQLException {
        return ".";
    }


    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }


    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }


    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }


    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }


    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }


    public boolean supportsUnion() throws SQLException {
        return true;
    }


    public boolean supportsUnionAll() throws SQLException {
        return true;
    }


    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return true;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return true;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return 16777208;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return 16777208;
    }

    public int getMaxColumnNameLength() throws SQLException {
        return 64;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return 64;
    }


    public int getMaxColumnsInIndex() throws SQLException {
        return 16;
    }


    public int getMaxColumnsInOrderBy() throws SQLException {
        return 64;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return 256;
    }


    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    public int getMaxConnections() throws SQLException {
        return 0;
    }


    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }


    public int getMaxIndexLength() throws SQLException {
        return 256;
    }
    public int getMaxSchemaNameLength() throws SQLException {
        return 32;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return 256;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    public int getMaxStatements() throws SQLException {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException {
        return 64;
    }

    public int getMaxTablesInSelect() throws SQLException {
        return 256;
    }

    public int getMaxUserNameLength() throws SQLException {
        return 16;
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    public boolean supportsTransactions() throws SQLException {
        return true;
    }


    public boolean supportsTransactionIsolationLevel(final int level)
            throws SQLException {
                switch (level) {
                    case Connection.TRANSACTION_READ_UNCOMMITTED:
                    case Connection.TRANSACTION_READ_COMMITTED:
                    case Connection.TRANSACTION_REPEATABLE_READ:
                    case Connection.TRANSACTION_SERIALIZABLE:
                        return true;
                    default:
                        return false;
                }
            }


    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException {
                return true;
            }

    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException {
                return false;
            }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }


    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }


    public ResultSet getProcedures(final String catalog, final String schemaPattern, final String procedureNamePattern)
            throws SQLException {

                return MySQLResultSet.EMPTY; /*TODO: return real ones */
            }

    public ResultSet getProcedureColumns(final String catalog, final String schemaPattern, final String procedureNamePattern,
            String columnNamePattern) throws SQLException {
                return MySQLResultSet.EMPTY; /*TODO: return real ones */
            }

    protected String getSchemaPattern(String schemaPattern) {
        if(schemaPattern != null && ((MySQLConnection)connection).noSchemaPattern == false) {
            return " AND table_schema LIKE '" + schemaPattern + "'";
        } else {
            return " AND table_schema LIKE IFNULL(database(), '%')";
        }
    }

    public ResultSet getSchemas() throws SQLException {
        final Statement stmt = connection.createStatement();
        return stmt.executeQuery("SELECT schema_name table_schem, catalog_name table_catalog " +
                "FROM information_schema.schemata");
    }


    public ResultSet getCatalogs() throws SQLException {
        final Statement stmt = connection.createStatement();
        return stmt.executeQuery("SELECT null as table_cat");
    }

    public ResultSet getTableTypes() throws SQLException {
        final Statement stmt = connection.createStatement();
        return stmt.executeQuery("SELECT DISTINCT(table_type) FROM information_schema.tables");
    }

    public ResultSet getColumnPrivileges(final String catalog, final String schema, final String table,
            final String columnNamePattern) throws SQLException {
      return MySQLResultSet.EMPTY;
            }


    public ResultSet getTablePrivileges(final String catalog, final String schemaPattern, final String tableNamePattern)
            throws SQLException {
                final Statement stmt = connection.createStatement();
                final String query = "SELECT null table_cat, " +
                        "table_schema table_schem, " +
                        "table_name, " +
                        "null grantor, " +
                        "user() grantee, " +
                        "'update' privilege, " +
                        "'yes' is_grantable " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema LIKE '" + ((schemaPattern == null) ? "%" : schemaPattern) + "'" +
                        " AND table_name LIKE '" + ((tableNamePattern == null) ? "%" : tableNamePattern) + "'";

                return stmt.executeQuery(query);
            }

    public ResultSet getVersionColumns(final String catalog, final String schema, final String table)
            throws SQLException {
        return MySQLResultSet.EMPTY;
    }

    public ResultSet getCrossReference(final String parentCatalog, final String parentSchema, final String parentTable,
            final String foreignCatalog, final String foreignSchema, final String foreignTable) throws SQLException {
               return MySQLResultSet.EMPTY;
            }

    public ResultSet getTypeInfo() throws SQLException {
        return MySQLResultSet.EMPTY;
    }

    public ResultSet getIndexInfo(final String catalog, final String schema, final String table,
            final boolean unique, final boolean approximate) throws SQLException {
                final String query = "SELECT null table_cat," +
                        "       table_schema table_schem," +
                        "       table_name," +
                        "       non_unique," +
                        "       table_schema index_qualifier," +
                        "       index_name," +
                        "       " + tableIndexOther + " type," +
                        "       seq_in_index ordinal_position," +
                        "       column_name," +
                        "       collation asc_or_desc," +
                        "       cardinality," +
                        "       null as pages," +
                        "       null as filter_condition" +
                        " FROM information_schema.statistics" +
                        " WHERE table_name='" + table + "' " +
                        ((schema != null) ? (" AND table_schema like '" + schema + "' ") : "") +
                        (unique ? " AND NON_UNIQUE = 0" : "") +
                        " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";
                final Statement stmt = connection.createStatement();
                return stmt.executeQuery(query);
            }
    public boolean supportsResultSetType(final int type) throws SQLException {
        return true;
    }


    public boolean supportsResultSetConcurrency(final int type, final int concurrency)
            throws SQLException {
                return concurrency == ResultSet.CONCUR_READ_ONLY;
            }

    public boolean ownUpdatesAreVisible(final int type) throws SQLException {
        return false;
    }


    public boolean ownDeletesAreVisible(final int type) throws SQLException {
        return false;
    }


    public boolean ownInsertsAreVisible(final int type) throws SQLException {
        return false;
    }


    public boolean othersUpdatesAreVisible(final int type) throws SQLException {
        return false;
    }


    public boolean othersDeletesAreVisible(final int type) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(final int type) throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(final int type) throws SQLException {
        return false;
    }


    public boolean deletesAreDetected(final int type) throws SQLException {
        return false;
    }


    public boolean insertsAreDetected(final int type) throws SQLException {
        return false;
    }


    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }


    public ResultSet getUDTs(final String catalog, final String schemaPattern, final String typeNamePattern, final int[] types)
            throws SQLException {
                return MySQLResultSet.EMPTY;
            }



    public Connection getConnection() throws SQLException {
        return connection;
    }


    public boolean supportsSavepoints() throws SQLException {
        return true;
    }


    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }


    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }


    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }


    public ResultSet getSuperTypes(final String catalog, final String schemaPattern, final String typeNamePattern)
            throws SQLException {

        return MySQLResultSet.EMPTY;
            }


    public ResultSet getSuperTables(final String catalog, final String schemaPattern, final String tableNamePattern)
            throws SQLException {
return MySQLResultSet.EMPTY;
            }


    public ResultSet getAttributes(final String catalog, final String schemaPattern, final String typeNamePattern,
            final String attributeNamePattern) throws SQLException {
return MySQLResultSet.EMPTY;
            }

    public boolean supportsResultSetHoldability(final int holdability)
            throws SQLException {
                return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
            }


    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }


    public int getDatabaseMajorVersion() throws SQLException {
        String components[] = version.split("\\.");
        return Integer.parseInt(components[0]);
    }


    public int getDatabaseMinorVersion() throws SQLException {
        String components[] = version.split("\\.");
        return Integer.parseInt(components[1]);
    }


    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }


    public int getSQLStateType() throws SQLException {
         return sqlStateSQL;
    }


    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }


    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }


    public java.sql.RowIdLifetime getRowIdLifetime() throws SQLException {
        return java.sql.RowIdLifetime.ROWID_UNSUPPORTED;
    }


    public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
        final String query = "SELECT schema_name table_schem, " +
                "null table_catalog " +
                "FROM information_schema.schemata " +
                (schemaPattern != null ? "WHERE schema_name like '" + schemaPattern + "'" : "") +
                " ORDER BY table_schem";
        final Statement stmt = connection.createStatement();
        return stmt.executeQuery(query);
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false; //TODO: look into this
    }


    public ResultSet getClientInfoProperties() throws SQLException {
        return MySQLResultSet.EMPTY; //TODO: look into this
    }


    public ResultSet getFunctions(final String catalog, final String schemaPattern, final String functionNamePattern)
            throws SQLException {
return MySQLResultSet.EMPTY; //TODO: fix

            }

    public ResultSet getFunctionColumns(final String catalog, final String schemaPattern, final String functionNamePattern,
            final String columnNamePattern) throws SQLException {
        return MySQLResultSet.EMPTY; //TODO: fix
            }


    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }


}
