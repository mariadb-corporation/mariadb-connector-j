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

import org.mariadb.jdbc.internal.common.Utils;
import org.mariadb.jdbc.internal.mysql.MySQLType;
import org.mariadb.jdbc.internal.mysql.MySQLValueObject;

import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MySQLDatabaseMetaData implements DatabaseMetaData {
    private String url;
    private MySQLConnection connection;
    private String databaseProductName = "MySQL";
    private String username;
    
    private  String dataTypeClause (String fullTypeColumnName){
        return
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
        " WHEN 'float' THEN "        + Types.REAL+
        " WHEN 'int' THEN IF( " + fullTypeColumnName + " like '%unsigned%', "+Types.INTEGER+","+ Types.INTEGER+ ")" + 
        " WHEN 'bigint' THEN "      + Types.BIGINT +
        " WHEN 'mediumint' THEN "   + Types.INTEGER +
        " WHEN 'null' THEN "        + Types.NULL +
        " WHEN 'set' THEN "         + Types.VARCHAR +
        " WHEN 'smallint' THEN IF( " + fullTypeColumnName + " like '%unsigned%', "+Types.SMALLINT+","+ Types.SMALLINT + ")" +
        " WHEN 'varchar' THEN "     + Types.VARCHAR +
        " WHEN 'varbinary' THEN "   + Types.VARBINARY +
        " WHEN 'char' THEN "        + Types.CHAR +
        " WHEN 'binary' THEN "      + Types.BINARY +
        " WHEN 'time' THEN "        + Types.TIME +
        " WHEN 'timestamp' THEN "   + Types.TIMESTAMP +
        " WHEN 'tinyint' THEN "  + (((connection.getProtocol().datatypeMappingFlags &  MySQLValueObject.TINYINT1_IS_BIT)== 0)? Types.TINYINT : "IF(" + fullTypeColumnName + "='tinyint(1)'," + Types.BIT + "," + Types.TINYINT + ") ") +  
        " WHEN 'year' THEN "  + (((connection.getProtocol().datatypeMappingFlags &  MySQLValueObject.YEAR_IS_DATE_TYPE)== 0)? Types.SMALLINT :Types.DATE) +  
        " ELSE "                    + Types.OTHER +  
        " END ";
    }

    /* Remove length from column type spec,convert to uppercase,  e.g  bigint(10) unsigned becomes BIGINT UNSIGNED */ 
    static String columnTypeClause(String columnName) {
    
        return 
                " UCASE(IF( " + columnName +  " LIKE '%(%)%', CONCAT (SUBSTRING( " + columnName + ",1, LOCATE('('," 
                + columnName +") - 1 ), SUBSTRING(" + columnName + ",1+locate(')'," + columnName + "))), " 
                + columnName + "))";
    }
    public MySQLDatabaseMetaData(Connection connection, String user, String url) {
        this.connection = (MySQLConnection)connection;
        this.username = user;
        this.url = url;
        this.connection.getProtocol().getServerVersion();
    }


    private ResultSet executeQuery(String sql) throws SQLException {
        MySQLResultSet rs =  (MySQLResultSet)connection.createStatement().executeQuery(sql);
        rs.setStatement(null); // bypass Hibernate statement tracking (CONJ-49)
        return rs;
    }
    
    
    private String escapeQuote(String s) {
        if (s == null)
            return "NULL";    
        return "'" + Utils.escapeString(s, connection.noBackslashEscapes) + "'";
    }

    /**
     * Generate part of the information schema query that restricts catalog names
     * In the driver, catalogs is the equivalent to MySQL schemas.
     * 
     * @param columnName - column name in the information schema table
     * @param catalog - catalog name.
     * 
     * This driver does not (always) follow JDBC standard for following special values, due 
     * to ConnectorJ compatibility
     * 
     * 1. empty string ("") - matches current catalog (i.e database).JDBC standard says
     * only tables without catalog should be returned - such tables do not exist in MySQL.
     * If there is no current catalog, then empty string matches any catalog.
     *   
     * 2. null  - if nullCatalogMeansCurrent=true (which is the default), then the handling is the same
     * as for "" . i.e return current catalog.
     *   
     * JDBC-conforming way would be to match any catalog with null parameter. This 
     * can be switched with nullCatalogMeansCurrent=false in the connection URL.
     * 
     * @return part of SQL query ,that restricts search for the catalog.
     */
    String catalogCond(String columnName, String catalog) {
        if (catalog == null && connection.nullCatalogMeansCurrent) {
            /* Treat null catalog as current */
            catalog = "";
        }
        
        if (catalog == null) {
            return "(1 = 1)"; 
        }
        if (catalog.equals("")) {
            return "(ISNULL(database()) OR (" + columnName + " = database()))";
        }
        return "(" + columnName + " = " + escapeQuote(catalog) + ")" ;
        
    }
    
     // Helper to generate  information schema queries with "like" or "equals" condition (typically  on table name)
    String patternCond(String columnName, String tableName) {
        if (tableName == null) {
            return "(1 = 1)";
        }
        String predicate = (tableName.indexOf('%') == -1 && tableName.indexOf('_') == -1)? "="  : "LIKE";
        return "(" + columnName + " " + predicate + " '" + Utils.escapeString(tableName, true) + "')";
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String sql =
                "SELECT A.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, NULL PK_NAME "
                 + " FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B"
                 + " WHERE A.COLUMN_KEY='pri' AND B.INDEX_NAME='PRIMARY' "
                 + " AND "
                 + catalogCond("A.TABLE_SCHEMA",catalog)
                 + " AND "
                 + catalogCond("B.TABLE_SCHEMA",catalog)
                 + " AND "
                 + patternCond("A.TABLE_NAME", table)
                 + " AND "
                 + patternCond("B.TABLE_NAME", table)
                 + " AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME "
                 + " ORDER BY A.COLUMN_NAME";

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
                String type = escapeQuote(mapTableTypes(types[i]));
                if (i == types.length -1)
                    sql += type + ")";
                else
                    sql += type + ",";
            }            
        }
        
        sql += " ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME";

        return executeQuery(sql);
    }
    
    public ResultSet getColumns(String catalog,  String schemaPattern,  String tableNamePattern,  String columnNamePattern)
        throws SQLException {

        String sql = 
        "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME," 
        + dataTypeClause("COLUMN_TYPE") + " DATA_TYPE,"
        + columnTypeClause("COLUMN_TYPE") +" TYPE_NAME, "
        + " CASE COLUMN_TYPE"
        + "  WHEN 'time' THEN 8"
        + "  WHEN 'date' THEN 10"       /* TODO : microseconds */
        + "  WHEN 'datetime' THEN 19"   
        + "  WHEN 'timestamp' THEN 19"
        + "  ELSE "
        + "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"+Integer.MAX_VALUE+"), NUMERIC_PRECISION) "
        + " END" 
        + " COLUMN_SIZE, 65535 BUFFER_LENGTH, NUMERIC_SCALE DECIMAL_DIGITS,"  
        + " 10 NUM_PREC_RADIX, IF(IS_NULLABLE = 'yes',1,0) NULLABLE,COLUMN_COMMENT REMARKS," 
        + " COLUMN_DEFAULT COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB,  "  
        + " LEAST(CHARACTER_OCTET_LENGTH," + Integer.MAX_VALUE + ") CHAR_OCTET_LENGTH,"
        + " ORDINAL_POSITION, IS_NULLABLE, NULL SCOPE_CATALOG, NULL SCOPE_SCHEMA, NULL SCOPE_TABLE, NULL SOURCE_DATA_TYPE,"
        + " IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT "
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
            throw new SQLException("'table' parameter in getExportedKeys cannot be null");
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
        + " KCU.REFERENCED_TABLE_NAME = " + escapeQuote(table) 
        + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
 
        return executeQuery(sql);
    }





   public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {

       // We avoid using information schema queries by default, because this appears to be an expensive
       // query (CONJ-41).
       if (table == null) {
         throw new SQLException("'table' parameter in getImportedKeys cannot be null");
       }

       if (catalog == null && connection.nullCatalogMeansCurrent) {
            /* Treat null catalog as current */
            catalog = "";
       }
       if (catalog == null) {
           return getImportedKeysUsingInformationSchema(catalog, schema, table);
       }

       if (catalog.equals("")) {
           catalog = connection.getCatalog();
           if (catalog == null || catalog.equals("")) {
               return getImportedKeysUsingInformationSchema(catalog, schema, table);
           }
       }

       try {
            return getImportedKeysUsingShowCreateTable(catalog, schema, table);
       } catch (Exception e) {
           // Likely, parsing failed, try out I_S query.
           return getImportedKeysUsingInformationSchema(catalog, schema, table);
       }
   }

    public ResultSet getImportedKeysUsingInformationSchema( String catalog,  String schema,  String table) throws SQLException {
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
        + " KCU.TABLE_NAME = " + escapeQuote(table) 
        + " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";
        
        return executeQuery(sql);
    }

    public  ResultSet  getImportedKeysUsingShowCreateTable( String catalog,  String schema,  String table) throws Exception {

      if (catalog == null || catalog.equals(""))
          throw new IllegalArgumentException("catalog");

      if (table == null || table.equals(""))
          throw new IllegalArgumentException("table");

      ResultSet rs = connection.createStatement().executeQuery("SHOW CREATE TABLE "  +
      MySQLConnection.quoteIdentifier(catalog) + "." +  MySQLConnection.quoteIdentifier(table));
      rs.next();
      String tableDef = rs.getString(2);
      return ShowCreateTableParser.getImportedKeys(tableDef, table, catalog, connection);
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, final boolean nullable)
            throws SQLException {
        
        if (table == null) {
            throw new SQLException("'table' parameter cannot be null in getBestRowIdentifier()");
        }
        
        String sql = 
        "SELECT " + DatabaseMetaData.bestRowUnknown + " SCOPE, COLUMN_NAME,"
        + dataTypeClause("COLUMN_TYPE") + " DATA_TYPE, DATA_TYPE TYPE_NAME,"
        + " IF(NUMERIC_PRECISION IS NULL, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) COLUMN_SIZE, 0 BUFFER_LENGTH," 
        + " NUMERIC_SCALE DECIMAL_DIGITS,"
        + " 1 PSEUDO_COLUMN" 
        + " FROM INFORMATION_SCHEMA.COLUMNS" 
        + " WHERE COLUMN_KEY IN('PRI', 'MUL', 'UNI')" 
        + " AND "
        + catalogCond("TABLE_SCHEMA",catalog)
        + " AND TABLE_NAME = " + escapeQuote(table);
        
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
        return connection.getProtocol().getServerVersion();
    }


    public String getDriverName() throws SQLException {
        return "mariadb-jdbc"; // TODO: get from constants file
    }

    public String getDriverVersion() throws SQLException {
        return Version.pomversion;
    }


    public int getDriverMajorVersion() {
        return 1;
    }

    public int getDriverMinorVersion() {
        return 1;
    }


    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return (connection.getLowercaseTableNames() == 0);
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return (connection.getLowercaseTableNames() == 1);
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return (connection.getLowercaseTableNames() == 2);
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return supportsMixedCaseIdentifiers();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return storesUpperCaseIdentifiers();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return storesLowerCaseIdentifiers();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return storesMixedCaseIdentifiers();
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
        + " NULL RESERVED1, NULL RESERVED2, NULL RESERVED3,"
        + " CASE ROUTINE_TYPE "
        +  "  WHEN 'FUNCTION' THEN " + procedureReturnsResult 
        +  "  WHEN 'PROCEDURE' THEN " + procedureNoResult  
        +  "  ELSE " + procedureResultUnknown
        + " END PROCEDURE_TYPE,"
        + " ROUTINE_COMMENT REMARKS, SPECIFIC_NAME "
        + " FROM INFORMATION_SCHEMA.ROUTINES "  
        + " WHERE " 
        + catalogCond("ROUTINE_SCHEMA" , catalog)
        + " AND "
        + patternCond("ROUTINE_NAME", procedureNamePattern)
        + "/* AND ROUTINE_TYPE='PROCEDURE' */";
    return executeQuery(sql);
    }

    
    /* Is INFORMATION_SCHEMA.PARAMETERS available ?*/
    boolean haveInformationSchemaParameters() {
    return connection.getProtocol().versionGreaterOrEqual(5, 5, 3);
    }
    
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        String sql;
        
        if (haveInformationSchemaParameters()) {
            /*
             *  Get info from information_schema.parameters 
             */
            sql = 
            "SELECT SPECIFIC_SCHEMA PROCEDURE_CAT, NULL PROCEDURE_SCHEM, SPECIFIC_NAME PROCEDURE_NAME,"
            +" PARAMETER_NAME COLUMN_NAME, " 
            + " CASE PARAMETER_MODE "
            + "  WHEN 'IN' THEN " + procedureColumnIn   
            + "  WHEN 'OUT' THEN " + procedureColumnOut
            + "  WHEN 'INOUT' THEN " + procedureColumnInOut 
            + "  ELSE IF(PARAMETER_MODE IS NULL," + procedureColumnReturn + "," + procedureColumnUnknown + ")"
            + " END COLUMN_TYPE,"
            + dataTypeClause("DTD_IDENTIFIER") + " DATA_TYPE,"
            + "DATA_TYPE TYPE_NAME,NUMERIC_PRECISION `PRECISION`,CHARACTER_MAXIMUM_LENGTH LENGTH,NUMERIC_SCALE SCALE,10 RADIX," 
            + procedureNullableUnknown +" NULLABLE,NULL REMARKS,NULL COLUMN_DEF,0 SQL_DATA_TYPE,0 SQL_DATETIME_SUB,"
            + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH ,ORDINAL_POSITION, '' IS_NULLABLE, SPECIFIC_NAME "
            + " FROM INFORMATION_SCHEMA.PARAMETERS "  
            + " WHERE " 
            + catalogCond("SPECIFIC_SCHEMA" , catalog)
            + " AND "+ patternCond("SPECIFIC_NAME", procedureNamePattern)
            + " AND "+ patternCond("PARAMETER_NAME", columnNamePattern)
            + " /* AND ROUTINE_TYPE='PROCEDURE' */ "
            + " ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION";
        } else {
            
            /* No information_schema.parameters
             * TODO : figure out what to do with older versions (get info via mysql.proc) 
             * For now, just a dummy result set is returned. 
             */
            sql = 
                "SELECT '' PROCEDURE_CAT, '' PROCEDURE_SCHEM , '' PROCEDURE_NAME,'' COLUMN_NAME, 0 COLUMN_TYPE,"
            + "0 DATA_TYPE,'' TYPE_NAME, 0 `PRECISION`,0 LENGTH, 0 SCALE,10 RADIX," 
            + "0 NULLABLE,NULL REMARKS,NULL COLUMN_DEF,0 SQL_DATA_TYPE,0 SQL_DATETIME_SUB,"
            + "0 CHAR_OCTET_LENGTH ,0 ORDINAL_POSITION, '' IS_NULLABLE, '' SPECIFIC_NAME "
            + " FROM DUAL "  
            + " WHERE 1=0 ";
        }
        return executeQuery(sql);
    }
    
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        
        String sql;
        if (haveInformationSchemaParameters()) {
        
            sql = "SELECT SPECIFIC_SCHEMA FUNCTION_CAT, NULL FUNCTION_SCHEM, SPECIFIC_NAME FUNCTION_NAME,"
            +" PARAMETER_NAME COLUMN_NAME, " 
            + " CASE PARAMETER_MODE "
            + "  WHEN 'IN' THEN " + functionColumnIn   
            + "  WHEN 'OUT' THEN " + functionColumnOut
            + "  WHEN 'INOUT' THEN " + functionColumnInOut   
            + "  ELSE " + functionReturn
            + " END COLUMN_TYPE,"
            + dataTypeClause("DTD_IDENTIFIER") + " DATA_TYPE,"
            + "DATA_TYPE TYPE_NAME,NUMERIC_PRECISION `PRECISION`,CHARACTER_MAXIMUM_LENGTH LENGTH,NUMERIC_SCALE SCALE,10 RADIX," 
            + procedureNullableUnknown +" NULLABLE,NULL REMARKS,"
            + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH ,ORDINAL_POSITION, '' IS_NULLABLE, SPECIFIC_NAME "
            + " FROM INFORMATION_SCHEMA.PARAMETERS "  
            + " WHERE " 
            + catalogCond("SPECIFIC_SCHEMA" , catalog)
            + " AND "+ patternCond("SPECIFIC_NAME", procedureNamePattern)
            + " AND "+ patternCond("PARAMETER_NAME", columnNamePattern)
            + " AND ROUTINE_TYPE='FUNCTION'"
            + " ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION";
        } else {
            /* 
             * No information_schema.parameters
             * TODO : figure out what to do with older versions (get info via mysql.proc) 
             * For now, just a dummy result set is returned. 
             */
            sql = 
            "SELECT '' FUNCTION_CAT, NULL FUNCTION_SCHEM, '' FUNCTION_NAME,"
            + " '' COLUMN_NAME, 0  COLUMN_TYPE, 0 DATA_TYPE,"
            + " '' TYPE_NAME,0 `PRECISION`,0 LENGTH, 0 SCALE,0 RADIX," 
            + " 0 NULLABLE,NULL REMARKS, 0 CHAR_OCTET_LENGTH , 0 ORDINAL_POSITION, " 
            + " '' IS_NULLABLE, '' SPECIFIC_NAME "
            + " FROM DUAL WHERE 1=0 " ;
        }
        return executeQuery(sql);
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
         + " TABLE_NAME = " + escapeQuote(table) 
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
            + " KCU.REFERENCED_TABLE_NAME = " + escapeQuote(parentTable) 
            + " AND "
            + " KCU.TABLE_NAME = " + escapeQuote(foreignTable)
            + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
     
        return executeQuery(sql);
    }

    public ResultSet getTypeInfo() throws SQLException {
        String[] columnNames  = {
                "TYPE_NAME","DATA_TYPE","PRECISION","LITERAL_PREFIX","LITERAL_SUFFIX",
                "CREATE_PARAMS","NULLABLE","CASE_SENSITIVE","SEARCHABLE","UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE","AUTO_INCREMENT","LOCAL_TYPE_NAME","MINIMUM_SCALE","MAXIMUM_SCALE",
                "SQL_DATA_TYPE","SQL_DATETIME_SUB","NUM_PREC_RADIX"
                };
        MySQLType [] columnTypes =  {
                MySQLType.VARCHAR, MySQLType.INTEGER,MySQLType.INTEGER,MySQLType.VARCHAR, MySQLType.VARCHAR,
                MySQLType.VARCHAR, MySQLType.INTEGER,MySQLType.BIT,MySQLType.SMALLINT,MySQLType.BIT,
                MySQLType.BIT, MySQLType.BIT, MySQLType.VARCHAR,MySQLType.SMALLINT,MySQLType.SMALLINT,
                MySQLType.INTEGER, MySQLType.INTEGER,MySQLType.INTEGER
                };
        
        String[][] data= {
        {"BIT","-7","1","","","","1","1","3","0","0","0","BIT","0","0","0","0","10"},
        {"BOOL","-7","1","","","","1","1","3","0","0","0","BOOL","0","0","0","0","10"},
        {"TINYINT","-6","3","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","TINYINT","0","0","0","0","10"},
        {"TINYINT UNSIGNED","-6","3","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","TINYINT UNSIGNED","0","0","0","0","10"},
        {"BIGINT","-5","19","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","BIGINT","0","0","0","0","10"},
        {"BIGINT UNSIGNED","-5","20","","","[(M)] [ZEROFILL]","1","0","3","1","0","1","BIGINT UNSIGNED","0","0","0","0","10"},
        {"LONG VARBINARY","-4","16777215","'","'","","1","1","3","0","0","0","LONG VARBINARY","0","0","0","0","10"},
        {"MEDIUMBLOB","-4","16777215","'","'","","1","1","3","0","0","0","MEDIUMBLOB","0","0","0","0","10"},
        {"LONGBLOB","-4","2147483647","'","'","","1","1","3","0","0","0","LONGBLOB","0","0","0","0","10"},
        {"BLOB","-4","65535","'","'","","1","1","3","0","0","0","BLOB","0","0","0","0","10"},
        {"TINYBLOB","-4","255","'","'","","1","1","3","0","0","0","TINYBLOB","0","0","0","0","10"},
        {"VARBINARY","-3","255","'","'","(M)","1","1","3","0","0","0","VARBINARY","0","0","0","0","10"},
        {"BINARY","-2","255","'","'","(M)","1","1","3","0","0","0","BINARY","0","0","0","0","10"},
        {"LONG VARCHAR","-1","16777215","'","'","","1","0","3","0","0","0","LONG VARCHAR","0","0","0","0","10"},
        {"MEDIUMTEXT","-1","16777215","'","'","","1","0","3","0","0","0","MEDIUMTEXT","0","0","0","0","10"},
        {"LONGTEXT","-1","2147483647","'","'","","1","0","3","0","0","0","LONGTEXT","0","0","0","0","10"},
        {"TEXT","-1","65535","'","'","","1","0","3","0","0","0","TEXT","0","0","0","0","10"},
        {"TINYTEXT","-1","255","'","'","","1","0","3","0","0","0","TINYTEXT","0","0","0","0","10"},
        {"CHAR","1","255","'","'","(M)","1","0","3","0","0","0","CHAR","0","0","0","0","10"},
        {"NUMERIC","2","65","","","[(M,D])] [ZEROFILL]","1","0","3","0","0","1","NUMERIC","-308","308","0","0","10"},
        {"DECIMAL","3","65","","","[(M,D])] [ZEROFILL]","1","0","3","0","0","1","DECIMAL","-308","308","0","0","10"},
        {"INTEGER","4","10","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","INTEGER","0","0","0","0","10"},
        {"INTEGER UNSIGNED","4","10","","","[(M)] [ZEROFILL]","1","0","3","1","0","1","INTEGER UNSIGNED","0","0","0","0","10"},
        {"INT","4","10","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","INT","0","0","0","0","10"},
        {"INT UNSIGNED","4","10","","","[(M)] [ZEROFILL]","1","0","3","1","0","1","INT UNSIGNED","0","0","0","0","10"},
        {"MEDIUMINT","4","7","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","MEDIUMINT","0","0","0","0","10"},
        {"MEDIUMINT UNSIGNED","4","8","","","[(M)] [ZEROFILL]","1","0","3","1","0","1","MEDIUMINT UNSIGNED","0","0","0","0","10"},
        {"SMALLINT","5","5","","","[(M)] [UNSIGNED] [ZEROFILL]","1","0","3","1","0","1","SMALLINT","0","0","0","0","10"},
        {"SMALLINT UNSIGNED","5","5","","","[(M)] [ZEROFILL]","1","0","3","1","0","1","SMALLINT UNSIGNED","0","0","0","0","10"},
        {"FLOAT","7","10","","","[(M|D)] [ZEROFILL]","1","0","3","0","0","1","FLOAT","-38","38","0","0","10"},
        {"DOUBLE","8","17","","","[(M|D)] [ZEROFILL]","1","0","3","0","0","1","DOUBLE","-308","308","0","0","10"},
        {"DOUBLE PRECISION","8","17","","","[(M,D)] [ZEROFILL]","1","0","3","0","0","1","DOUBLE PRECISION","-308","308","0","0","10"},
        {"REAL","8","17","","","[(M,D)] [ZEROFILL]","1","0","3","0","0","1","REAL","-308","308","0","0","10"},
        {"VARCHAR","12","255","'","'","(M)","1","0","3","0","0","0","VARCHAR","0","0","0","0","10"},
        {"ENUM","12","65535","'","'","","1","0","3","0","0","0","ENUM","0","0","0","0","10"},
        {"SET","12","64","'","'","","1","0","3","0","0","0","SET","0","0","0","0","10"},
        {"DATE","91","10","'","'","","1","0","3","0","0","0","DATE","0","0","0","0","10"},
        {"TIME","92","18","'","'","[(M)]","1","0","3","0","0","0","TIME","0","0","0","0","10"},
        {"DATETIME","93","27","'","'","[(M)]","1","0","3","0","0","0","DATETIME","0","0","0","0","10"},
        {"TIMESTAMP","93","27","'","'","[(M)]","1","0","3","0","0","0","TIMESTAMP","0","0","0","0","10"}
        };

        return MySQLResultSet.createResultSet(columnNames, columnTypes, data, connection.getProtocol());
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table,
            boolean unique,boolean approximate) throws SQLException {
    
        String sql = 
            "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, "
            + " TABLE_SCHEMA INDEX_QUALIFIER, INDEX_NAME, 3 TYPE,"
            + " SEQ_IN_INDEX ORDINAL_POSITION, COLUMN_NAME, COLLATION ASC_OR_DESC," 
            + " CARDINALITY, NULL PAGES, NULL FILTER_CONDITION"
            + " FROM INFORMATION_SCHEMA.STATISTICS" 
            + " WHERE TABLE_NAME = " + escapeQuote(table)
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
       return connection.getProtocol().getMajorServerVersion();
    }


    public int getDatabaseMinorVersion() throws SQLException {
        return connection.getProtocol().getMinorServerVersion();
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
        + " ROUTINE_COMMENT REMARKS," + functionNoTable + " FUNCTION_TYPE, SPECIFIC_NAME "
        + " FROM INFORMATION_SCHEMA.ROUTINES "  
        + " WHERE " 
        + catalogCond("ROUTINE_SCHEMA" , catalog)
        + " AND "
        + patternCond("ROUTINE_NAME", functionNamePattern)
        + " AND ROUTINE_TYPE='FUNCTION'";
        
        return executeQuery(sql);
    }




    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return false;
    }


}

/*  Create table parsing stuff */

/*
 Identifier, i.e table, or column name. Put into ` quotes in SHOW CREATE TABLE. Can be "multi-part", i.e `schema`.`table`
 */
class Identifier {
    public String schema;
    public String name;
    public String toString() {
        if (schema != null)
            return schema + "." + name;
        return name;
    }
}

/*
 Parse foreign key constraints from "SHOW CREATE TABLE". The single usage of this class is to speedup getImportedKeys
 (I_S appear to be too slow, see CONJ-41)
*/
class ShowCreateTableParser {
     // Extract identifier quoted string from input String.
     // Return new position, or -1 on error
     static int skipWhite(char [] s, int startPos) {
         for(int i = startPos; i < s.length; i++) {
             if(!Character.isWhitespace(s[i])) {
                 return i;
             }
         }
         return s.length;
     }

     static int parseIdentifier(char[] s, int startPos, Identifier identifier) throws ParseException {
        int pos = skipWhite(s , startPos);
        if (s[pos] != '`')
            throw new ParseException(new String(s),pos);
        pos++;
        StringBuffer sb = new StringBuffer();
        int nQuotes=0;
        for(; pos < s.length; pos++) {
            char ch = s[pos];
            if (ch  == '`') {
                nQuotes++;
            } else {
                for (int j = 0; j < nQuotes/2; j++)
                    sb.append('`');
                if (nQuotes %2 == 1) {
                    if (ch == '.') {
                        if (identifier.schema != null)
                            throw new ParseException(new String(s), pos);
                        identifier.schema = sb.toString();
                        return parseIdentifier(s, pos + 1,identifier);
                    }
                    identifier.name = sb.toString();
                    return pos;
                }
                nQuotes = 0;
                sb.append(ch);
            }
        }
        throw new ParseException(new String(s),startPos);
     }
    static int parseIdentifierList(char[] s, int startPos,List<Identifier> list) throws ParseException {
        int pos = skipWhite(s , startPos);
        if (s[pos] != '(') {
            throw new ParseException(new String(s),pos);
        }
        pos++;
        for(;;) {
            pos = skipWhite(s, pos);
            char ch = s[pos];
            switch (ch) {
                case ')':
                    return pos +1 ;
                case '`':
                    Identifier id = new Identifier();
                    pos = parseIdentifier(s, pos, id);
                    list.add(id);
                    break;
                case ',':
                    pos++;
                    break;
                default:
                    throw new ParseException(new String(s,startPos, s.length - startPos),startPos);
            }
        }
    }
     static int skipKeyword(char[] s, int startPos, String keyword)  throws ParseException{
         int pos = skipWhite(s , startPos);
         for (int i = 0 ; i < keyword.length(); i++,pos++){
             if (s[pos] != keyword.charAt(i)) {
                throw new ParseException(new String(s),pos);
             }
         }
         return pos;
     }

     static int getImportedKeyAction(String s) {
         if(s == null)
             return DatabaseMetaData.importedKeyRestrict;
         if (s.equals("NO ACTION"))
             return DatabaseMetaData.importedKeyNoAction;
         if (s.equals("CASCADE"))
             return DatabaseMetaData.importedKeyCascade;
         if (s.equals("SET NULL"))
             return DatabaseMetaData.importedKeySetNull;
         if (s.equals("SET DEFAULT"))
             return DatabaseMetaData.importedKeySetDefault;
         if (s.equals("RESTRICT"))
             return DatabaseMetaData.importedKeyRestrict;
         throw new AssertionError("should not happen");
     }


     public static ResultSet getImportedKeys(String tableDef, String tableName, String catalog, MySQLConnection c) throws ParseException {
         String[] columnNames  = {
                 "PKTABLE_CAT","PKTABLE_SCHEM", "PKTABLE_NAME",
                 "PKCOLUMN_NAME","FKTABLE_CAT","FKTABLE_SCHEM",
                 "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
                 "UPDATE_RULE","DELETE_RULE","FK_NAME",
                 "PK_NAME","DEFERRABILITY"
                 };
         MySQLType [] columnTypes =  {
                 MySQLType.VARCHAR, MySQLType.NULL, MySQLType.VARCHAR,
                 MySQLType.VARCHAR, MySQLType.VARCHAR, MySQLType.NULL,
                 MySQLType.VARCHAR, MySQLType.VARCHAR, MySQLType.SMALLINT,
                 MySQLType.SMALLINT, MySQLType.SMALLINT, MySQLType.VARCHAR,
                 MySQLType.NULL,MySQLType.SMALLINT};

         String[] parts = tableDef.split("\n");

         List<String[]> data = new ArrayList<String[]>();

         for (String p:parts) {
              //System.out.println("--" + p);
              p = p.trim();
              if (!p.startsWith("CONSTRAINT") && !p.contains("FOREIGN KEY"))
                  continue;
              char [] s = p.toCharArray();

              Identifier constraintName = new Identifier();
              Identifier pkTable = new Identifier();
              List<Identifier> foreignKeyCols = new ArrayList<Identifier>();
              List<Identifier> primaryKeyCols = new ArrayList<Identifier>();

              int pos = skipKeyword(s, 0, "CONSTRAINT");
              pos = parseIdentifier(s, pos, constraintName);
              pos = skipKeyword(s, pos, "FOREIGN KEY");
              pos = parseIdentifierList(s, pos, foreignKeyCols);
              pos = skipKeyword(s, pos, "REFERENCES");
              pos = parseIdentifier(s, pos, pkTable);
              parseIdentifierList(s, pos, primaryKeyCols);
              if (primaryKeyCols.size() != foreignKeyCols.size()) {
                  throw new ParseException(tableDef,0);
              }
              int onUpdateReferenceAction = DatabaseMetaData.importedKeyRestrict;
              int onDeleteReferenceAction = DatabaseMetaData.importedKeyRestrict;


             for (String referenceAction : new String[] {"RESTRICT", "CASCADE",  "SET NULL", "NO ACTION"}) {
                  if (p.contains("ON UPDATE " + referenceAction))
                      onUpdateReferenceAction = getImportedKeyAction(referenceAction);
                  if (p.contains("ON DELETE " + referenceAction))
                      onDeleteReferenceAction =  getImportedKeyAction(referenceAction);
             }

             for(int i = 0; i < primaryKeyCols.size(); i++) {

                 String[] row = new String[columnNames.length];
                 row[0] =  pkTable.schema;
                 if (row[0] == null) {
                     row[0] = catalog;
                 }
                 row[1] = null;
                 row[2] = pkTable.name;
                 row[3] = primaryKeyCols.get(i).name;
                 row[4] = catalog;
                 row[5] = null;
                 row[6] = tableName;
                 row[7] = foreignKeyCols.get(i).name;
                 row[8] = Integer.toString(i+1);
                 row[9] = Integer.toString(onUpdateReferenceAction);
                 row[10] = Integer.toString(onDeleteReferenceAction);
                 row[11] = constraintName.name;
                 row[12] = null;
                 row[13] = Integer.toString(DatabaseMetaData.importedKeyInitiallyImmediate);
                 data.add(row);
             }
         }
         String[][] arr = data.toArray(new String[0][]);

         /* Sort array by PKTABLE_CAT, PKTABLE_NAME, and KEY_SEQ.*/
         Arrays.sort(arr, new Comparator<String[]>() {
             @Override
             public int compare(String[] row1, String[] row2) {
                 int result = row1[0].compareTo(row2[0]);   //PKTABLE_CAT
                 if (result == 0){
                    result = row1[2].compareTo(row2[2]);   //PKTABLE_NAME
                    if (result == 0) {
                        result = row1[8].length() - row2[8].length();  // KEY_SEQ
                        if (result == 0)
                            result =  row1[8].compareTo(row2[8]);
                    }
                 }
                 return result;
             }
         });
         ResultSet ret =  MySQLResultSet.createResultSet(columnNames,columnTypes,arr, c.getProtocol());
         return ret;
      }
 }
