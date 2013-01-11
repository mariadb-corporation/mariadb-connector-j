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

import org.mariadb.jdbc.internal.common.Utils;


public class MySQLDatabaseMetaData implements DatabaseMetaData {
    private String version;
    private String url;
    private MySQLConnection connection;
    private String databaseProductName = "MySQL";
    private String username;
    
    private static final String dataTypeClause = 
    		            " CASE data_type" +
                        " WHEN 'bit' THEN "         + Types.BIT +
                        " WHEN 'tinyblob' THEN "    + Types.LONGVARBINARY +
                        " WHEN 'mediumblob' THEN "  + Types.LONGVARBINARY +
                        " WHEN 'longblob' THEN "    + Types.LONGVARBINARY +
                        " WHEN 'blob' THEN "        + Types.LONGVARBINARY +
                        " WHEN 'tinytext' THEN "    + Types.LONGVARCHAR +
                        " WHEN 'mediumtext' THEN "  + Types.LONGVARCHAR +
                        " WHEN 'longtext' THEN "    + Types.LONGVARCHAR +
                        " WHEN 'text' THEN "        + Types.LONGVARCHAR +
                        " WHEN 'date' THEN "        + Types.DATE +
                        " WHEN 'datetime' THEN "    + Types.TIMESTAMP +
                        " WHEN 'decimal' THEN "     + Types.DECIMAL +
                        " WHEN 'double' THEN "      + Types.DOUBLE +
                        " WHEN 'enum' THEN "        + Types.VARCHAR +
                        " WHEN 'float' THEN "       + Types.FLOAT +
                        " WHEN 'int' THEN "         + Types.INTEGER +
                        " WHEN 'bigint' THEN "      + Types.BIGINT +
                        " WHEN 'mediumint' THEN "   + Types.INTEGER +
                        " WHEN 'null' THEN "        + Types.NULL +
                        " WHEN 'set' THEN "         + Types.VARCHAR +
                        " WHEN 'smallint' THEN "    + Types.SMALLINT +
                        " WHEN 'varchar' THEN "     + Types.VARCHAR +
                        " WHEN 'varbinary' THEN "   + Types.VARBINARY +
                        " WHEN 'char' THEN "        + Types.CHAR +
                        " WHEN 'binary' THEN "      + Types.BINARY +
                        " WHEN 'time' THEN "        + Types.TIME +
                        " WHEN 'timestamp' THEN "   + Types.TIMESTAMP +
                        " WHEN 'tinyint' THEN "     + Types.TINYINT +
                        " WHEN 'year' THEN "        + Types.SMALLINT +
                        " ELSE "                    + Types.OTHER +  
                        " END ";

    public MySQLDatabaseMetaData(Connection connection, String user, String url) {
        this.connection = (MySQLConnection)connection;
        this.username = user;
        this.url = url;
    }


    private ResultSet executeQuery(String sql) throws SQLException {
    	return connection.createStatement().executeQuery(sql);
    }
    
    
    private String escapeString(String s) {
    	return Utils.escapeString(s, connection.noBackslashEscapes);
    }
    
    String catalogCond(String columnName, String catalog) {
    	if (catalog == null){
    		return "(1 = 1)"; 
    	}
    	if (catalog.equals("")) {
    		return "(ISNULL(database()) OR (" + columnName + " = database()))";
    	}
    	return "(" + columnName + " = '" + escapeString(catalog) + "')" ;
    	
    }
    
     // Helper to generate  information schema queries with "like" condition (typically  on table name)
    String patternCond(String columnName, String tableName) {
    	if (tableName == null) {
    		return "(1 = 1)";
    	}
    	return "(" + columnName + " LIKE '" + Utils.escapeString(tableName, true) + "')";
    }
    
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        
        String sql = 
        "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION KEY_SEQ, NULL PK_NAME " 
        + " FROM INFORMATION_SCHEMA.COLUMNS"
        + " WHERE COLUMN_KEY='pri'"
        + " AND " 
        + catalogCond("TABLE_SCHEMA",catalog) 
        + " AND " 
        + patternCond("TABLE_NAME", table)
        + " ORDER BY column_name";

        return executeQuery(sql);
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

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) 
    		throws SQLException {

        String sql = 
         "SELECT TABLE_SCHEMA TABLE_CAT, NULL  TABLE_SCHEM,  TABLE_NAME, TABLE_TYPE, TABLE_COMMENT REMARKS,"
         + " NULL TYPE_CAT, NULL TYPE_SCHEM, NULL TYPE_NAME, NULL SELF_REFERENCING_COL_NAME, " 
         + " NULL REF_GENERATION" 
         + " FROM INFORMATION_SCHEMA.TABLES "
         + " WHERE "
         + catalogCond("TABLE_SCHEMA",catalog)  
         + " AND "
         + patternCond("TABLE_NAME", tableNamePattern);
        
        if (types != null && types.length > 0) {
        	sql += " AND TABLE_TYPE IN (" ; 
        	for (int i=0 ; i < types.length; i++) {
        		if(types[i] == null)
        			continue;
        		String type = escapeString(mapTableTypes(types[i]));
        		if (i == types.length -1)
        			sql += "'" + type +"')";
        		else
        			sql += "'" + type + "',";
        	}			
        }
        
        sql += " ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME";

        return executeQuery(sql);
    }
    
    public ResultSet getColumns(String catalog,  String schemaPattern,  String tableNamePattern,  String columnNamePattern)
        throws SQLException {

        String sql = 
        "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME," 
        + dataTypeClause + " DATA_TYPE,"
        + " COLUMN_TYPE TYPE_NAME, CHARACTER_MAXIMUM_LENGTH COLUMN_SIZE, 0 BUFFER_LENGTH, NUMERIC_PRECISION DECIMAL_DIGITS,"  
        + " NUMERIC_SCALE NUM_PREC_RADIX, IF(IS_NULLABLE = 'yes',1,0) NULLABLE,COLUMN_COMMENT REMARKS," 
        + " COLUMN_DEFAULT COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB,  CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH,"
        + " ORDINAL_POSITION, IS_NULLABLE, NULL SCOPE_CATALOG, NULL SCOPE_SCHEMA, NULL SCOPE_TABLE, NULL SOURCE_DATA_TYPE,"
        + " '' IS_AUTOINCREMENT "
        + " FROM INFORMATION_SCHEMA.COLUMNS  WHERE "
        + catalogCond("TABLE_SCHEMA", catalog)
        + " AND "
        + patternCond("TABLE_NAME", tableNamePattern)
        + " AND "
        + patternCond("COLUMN_NAME", columnNamePattern) 
        + " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
        
        return executeQuery(sql);
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    	if (table == null) {
    		throw new SQLException("'table' parameter in getImportedKeys cannot be null"); 
    	}
        String sql =
        "SELECT KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM,  KCU.REFERENCED_TABLE_NAME PKTABLE_NAME," 
        + " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, KCU.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
        + " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
        + " CASE update_rule "
        + "   WHEN 'RESTRICT' THEN 1" 
        + "   WHEN 'NO ACTION' THEN 3" 
        + "   WHEN 'CASCADE' THEN 0" 
        + "   WHEN 'SET NULL' THEN 2"
        + "   WHEN 'SET DEFAULT' THEN 4"
        + " END UPDATE_RULE,"
        + " CASE DELETE_RULE" 
        + "  WHEN 'RESTRICT' THEN 1"
        + "  WHEN 'NO ACTION' THEN 3"
        + "  WHEN 'CASCADE' THEN 0"
        + "  WHEN 'SET NULL' THEN 2"
        + "  WHEN 'SET DEFAULT' THEN 4"
        + " END DELETE_RULE,"
        + " RC.CONSTRAINT_NAME FK_NAME,"
        + " NULL PK_NAME,"
        + " 6 DEFERRABILITY"
        + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
        + " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
        + " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
        + " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
        + " WHERE "
        + catalogCond("KCU.REFERENCED_TABLE_SCHEMA", catalog)
        + " AND "
        + " KCU.REFERENCED_TABLE_NAME = '" + escapeString(table) + "'" +
        " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
 
        return executeQuery(sql);
    }
    
    public ResultSet getImportedKeys( String catalog,  String schema,  String table) throws SQLException {
    	if (table == null) {
    		throw new SQLException("'table' parameter in getImportedKeys cannot be null"); 
    	}
        String sql = 
          "SELECT KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM,  KCU.REFERENCED_TABLE_NAME PKTABLE_NAME," 
	    + " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, KCU.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
	    + " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
	    + " CASE update_rule "
	    + "   WHEN 'RESTRICT' THEN 1" 
	    + "   WHEN 'NO ACTION' THEN 3" 
	    + "   WHEN 'CASCADE' THEN 0" 
	    + "   WHEN 'SET NULL' THEN 2"
	    + "   WHEN 'SET DEFAULT' THEN 4"
	    + " END UPDATE_RULE,"
	    + " CASE DELETE_RULE" 
	    + "  WHEN 'RESTRICT' THEN 1"
	    + "  WHEN 'NO ACTION' THEN 3"
	    + "  WHEN 'CASCADE' THEN 0"
	    + "  WHEN 'SET NULL' THEN 2"
	    + "  WHEN 'SET DEFAULT' THEN 4"
	    + " END DELETE_RULE,"
	    + " RC.CONSTRAINT_NAME FK_NAME,"
	    + " NULL PK_NAME,"
	    + " 6 DEFERRABILITY"
	    + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
	    + " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
	    + " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
	    + " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
	    + " WHERE "
	    + catalogCond("KCU.TABLE_SCHEMA", catalog)
        + " AND "
        + " KCU.TABLE_NAME = '" + escapeString(table) + "'" 
        + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        
        return executeQuery(sql);
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, final boolean nullable)
            throws SQLException {
    	
    	if (table == null) {
    		throw new SQLException("'table' parameter cannot be null in getBestRowIdentifier()");
    	}
    	
    	String sql = 
    	"SELECT " + DatabaseMetaData.bestRowUnknown + " SCOPE, COLUMN_NAME,"
        + dataTypeClause + " DATA_TYPE, DATA_TYPE TYPE_NAME,"
        + " IF(NUMERIC_PRECISION IS NULL, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) COLUMN_SIZE, 0 BUFFER_LENGTH," 
        + " NUMERIC_SCALE DECIMAL_DIGITS,"
        + " 1 PSEUDO_COLUMN" 
        + " FROM INFORMATION_SCHEMA.COLUMNS" 
        + " WHERE COLUMN_KEY IN('PRI', 'MUL', 'UNI')" 
        + " AND "
        + catalogCond("TABLE_SCHEMA",catalog)
        + " AND TABLE_NAME = '" + escapeString(table)+ "'";
    	
        return executeQuery(sql);
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    public ResultSet getPseudoColumns(String catalog,  String schemaPattern, String tableNamePattern,
    		String columnNamePattern) throws SQLException {
    	
        return connection.createStatement().executeQuery(
        		"SELECT ' ' TABLE_CAT, ' ' TABLE_SCHEM," +
        		"' ' TABLE_NAME, ' ' COLUMN_NAME, 0 DATA_TYPE, 0 COLUMN_SIZE, 0 DECIMAL_DIGITS," + 
        		"10 NUM_PREC_RADIX, ' ' COLUMN_USAGE,  ' ' REMARKS, 0 CHAR_OCTET_LENGTH, 'YES' IS_NULLABLE FROM DUAL " +
        		"WHERE 1=0");
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return true;
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
    	ResultSet rs  = connection.createStatement().executeQuery("select version()");
    	rs.next();
    	String version = rs.getString(1);
    	rs.close();
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

    public boolean supportsConvert(int fromType, int toType)
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
        return "";
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


    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
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


    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    	return true;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    	return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }


    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    		
    /* Helper to generate  information schema with "equality" condition (typically on catalog name)
     */
    

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
    	
    
    	String sql = 
    	"SELECT ROUTINE_SCHEMA PROCEDURE_CAT,NULL PROCEDURE_SCHEM, ROUTINE_NAME PROCEDURE_NAME,"  
        + " NULL NUM_INPUT_PARAMS, NULL NUM_OUTPUT_PARAMS, NULL NUM_RESULT_PARAMS,"         
        + " CASE ROUTINE_TYPE  WHEN 'FUNCTION' THEN 2  WHEN 'PROCEDURE' THEN 1 ELSE 0 END PROCEDURE_TYPE,"
        + " ROUTINE_COMMENT REMARKS, SPECIFIC_NAME "
        + " FROM INFORMATION_SCHEMA.ROUTINES "  
    	+ " WHERE " 
        + catalogCond("ROUTINE_SCHEMA" , catalog)
        + " AND "
        + patternCond("ROUTINE_NAME", procedureNamePattern)
        + " AND ROUTINE_TYPE='PROCEDURE'";
    	
    	return executeQuery(sql);
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
                return MySQLResultSet.EMPTY; /*TODO: return real ones */
    }

    
    public ResultSet getSchemas() throws SQLException {
        return executeQuery(
        		"SELECT '' TABLE_SCHEM, '' TABLE_catalog  FROM DUAL WHERE 1=0");
    }


    public ResultSet getCatalogs() throws SQLException {
        return executeQuery(
        		"SELECT SCHEMA_NAME TABLE_CAT FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY 1");
    }

    public ResultSet getTableTypes() throws SQLException {
        return executeQuery(
        		"SELECT 'BASE TABLE' TABLE_TYPE UNION SELECT 'SYSTEM VIEW' TABLE_TYPE UNION SELECT 'VIEW' TABLE_TYPE");
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
            String columnNamePattern) throws SQLException {
    	
    	if(table == null) {
    		throw new SQLException("'table' parameter must not be null");
    	}
    	String sql = 
	       "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME,"
		 + " COLUMN_NAME, NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE, IS_GRANTABLE FROM "
		 + " INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE "
		 + catalogCond("TABLE_SCHEMA", catalog)
		 + " AND "
		 + " TABLE_NAME = '" + escapeString(table) + "'"
		 + " AND "
		 + patternCond("COLUMN_NAME",columnNamePattern) 
		 + " ORDER BY COLUMN_NAME, PRIVILEGE_TYPE";
    	
      return executeQuery(sql);
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
      String sql = 
      "SELECT TABLE_SCHEMA TABLE_CAT,NULL  TABLE_SCHEM, TABLE_NAME, NULL GRANTOR," 
      + "GRANTEE, PRIVILEGE_TYPE  PRIVILEGE, IS_GRANTABLE  FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
      + " WHERE "
      + catalogCond("TABLE_SCHEMA", catalog)   
      + " AND "
      + patternCond("TABLE_NAME",tableNamePattern)
      + "ORDER BY TABLE_SCHEMA, TABLE_NAME,  PRIVILEGE_TYPE ";
        
      return executeQuery(sql);
   }

    public ResultSet getVersionColumns(String catalog, String schema, String table)
            throws SQLException {
        String sql = 
        	"SELECT 0 SCOPE, ' ' COLUMN_NAME, 0 DATA_TYPE,"  
            + " ' ' TYPE_NAME, 0 COLUMN_SIZE, 0 BUFFER_LENGTH,"  
            + " 0 DECIMAL_DIGITS, 0 PSEUDO_COLUMN "
            + " FROM DUAL WHERE 1 = 0";
        return executeQuery(sql);
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) 
            		throws SQLException {
    	
    	 String sql =
	        "SELECT KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM,  KCU.REFERENCED_TABLE_NAME PKTABLE_NAME," 
	        + " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, KCU.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
	        + " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
	        + " CASE update_rule "
	        + "   WHEN 'RESTRICT' THEN 1" 
	        + "   WHEN 'NO ACTION' THEN 3" 
	        + "   WHEN 'CASCADE' THEN 0" 
	        + "   WHEN 'SET NULL' THEN 2"
	        + "   WHEN 'SET DEFAULT' THEN 4"
	        + " END UPDATE_RULE,"
	        + " CASE DELETE_RULE" 
	        + "  WHEN 'RESTRICT' THEN 1"
	        + "  WHEN 'NO ACTION' THEN 3"
	        + "  WHEN 'CASCADE' THEN 0"
	        + "  WHEN 'SET NULL' THEN 2"
	        + "  WHEN 'SET DEFAULT' THEN 4"
	        + " END DELETE_RULE,"
	        + " RC.CONSTRAINT_NAME FK_NAME,"
	        + " NULL PK_NAME,"
	        + " 6 DEFERRABILITY"
	        + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
	        + " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
	        + " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
	        + " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
	        + " WHERE "
	        + catalogCond("KCU.REFERENCED_TABLE_SCHEMA", parentCatalog)
	        + " AND "
	        + catalogCond("KCU.TABLE_SCHEMA", foreignCatalog) 
	        + " AND "
	        + " KCU.REFERENCED_TABLE_NAME = '" + escapeString(parentTable) + "'"
	        + " AND "
	        + " KCU.TABLE_NAME = '" + escapeString(foreignTable) + "'"
	        + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
	 
	    return executeQuery(sql);
    }

    public ResultSet getTypeInfo() throws SQLException {
        return MySQLResultSet.EMPTY;
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table,
            boolean unique,boolean approximate) throws SQLException {
    
    	String sql = 
    		"SELECT  TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, "
    		+ " TABLE_SCHEMA INDEX_QUALIFIER, index_name, 3 type,"
    		+ " SEQ_IN_INDEX ORDINAL_POSITION, COLUMN_NAME, COLLATION ASC_OR_DESC," 
            + " CARDINALITY, NULL PAGES, NULL FILTER_CONDITION"
    		+ " FROM INFORMATION_SCHEMA.STATISTICS" 
            + " WHERE table_name = '" + escapeString(table) + "'"
            + " AND "
            + catalogCond("TABLE_SCHEMA",catalog)
            + ((unique) ? " AND NON_UNIQUE = 0" : "") 
            + " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";
            
            return executeQuery(sql);
    }
    
    
    public boolean supportsResultSetType(int type) throws SQLException {
        return true;
    }


    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
         return concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }


    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }


    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }


    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }


    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }


    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }


    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }


    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }


    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
    	String sql = 
    	"SELECT ' ' TYPE_CAT, NULL TYPE_SCHEM, ' ' TYPE_NAME, ' ' CLASS_NAME, 0 DATA_TYPE, ' ' REMARKS, 0 BASE_TYPE"
    	+ " FROM DUAL WHERE 1=0";
    	
        return executeQuery(sql);
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


    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
    	String sql = 
		"SELECT  ' ' TYPE_CAT, NULL TYPE_SCHEM, ' ' TYPE_NAME, ' ' SUPERTYPE_CAT, ' ' SUPERTYPE_SCHEM, ' '  SUPERTYPE_NAME" 
		+ " FROM DUAL WHERE 1=0";
    	
    	return executeQuery(sql);	
    }


    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
    	String sql = 
		"SELECT  ' ' TABLE_CAT, ' ' TABLE_SCHEM, ' ' TABLE_NAME, ' ' SUPERTABLE_NAME FROM DUAL WHERE 1=0";
    	return executeQuery(sql); 					
    }


    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
 
      String sql = 
        "SELECT ' ' TYPE_CAT, ' ' TYPE_SCHEM, ' ' TYPE_NAME, ' ' ATTR_NAME, 0 DATA_TYPE,"
		+ " ' ' ATTR_TYPE_NAME, 0 ATTR_SIZE, 0 DECIMAL_DIGITS, 0 NUM_PREC_RADIX, 0 NULLABLE,"
		+ " ' ' REMARKS, ' ' ATTR_DEF,  0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB, 0 CHAR_OCTET_LENGTH,"
		+ " 0 ORDINAL_POSITION, ' ' IS_NULLABLE, ' ' SCOPE_CATALOG, ' ' SCOPE_SCHEMA, ' ' SCOPE_TABLE," 
		+ " 0 SOURCE_DATA_TYPE"
		+ " FROM DUAL " 
		+ " WHERE 1=0";
     
     return executeQuery(sql);
     
     
    }

    public boolean supportsResultSetHoldability(int holdability)
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


    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return executeQuery("SELECT  ' ' table_schem, ' ' table_catalog FROM DUAL WHERE 1=0");
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false; //TODO: look into this
    }


    public ResultSet getClientInfoProperties() throws SQLException {
    	String sql = "SELECT ' ' NAME, 0 MAX_LEN, ' ' DEFAULT_VALUE, ' ' DESCRIPTION FROM DUAL WHERE 1=0";
        return executeQuery(sql);
    }


    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
    	String sql = 
    	"SELECT ROUTINE_SCHEMA FUNCTION_CAT,NULL FUNCTION_SCHEM, ROUTINE_NAME FUNCTION_NAME,"  
        + " NULL NUM_INPUT_PARAMS, NULL NUM_OUTPUT_PARAMS, NULL NUM_RESULT_PARAMS,"         
        + " ROUTINE_COMMENT REMARKS," + functionNoTable + " FUNCTION_TYPE, SPECIFIC_NAME "
        + " FROM INFORMATION_SCHEMA.ROUTINES "  
    	+ " WHERE " 
        + catalogCond("ROUTINE_SCHEMA" , catalog)
        + " AND "
        + patternCond("ROUTINE_NAME", functionNamePattern)
        + " AND ROUTINE_TYPE='FUNCTION'";
    	
    	return executeQuery(sql);
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return MySQLResultSet.EMPTY; //TODO: fix
            }


    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }


}
