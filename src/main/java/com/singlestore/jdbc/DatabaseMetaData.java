// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.client.result.Result;
import com.singlestore.jdbc.util.Version;
import com.singlestore.jdbc.util.VersionFactory;
import com.singlestore.jdbc.util.constants.ServerStatus;
import java.sql.PseudoColumnUsage;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;

public class DatabaseMetaData implements java.sql.DatabaseMetaData {

  public static final String DRIVER_NAME = "SingleStore JDBC";

  private final com.singlestore.jdbc.Connection connection;
  private final Configuration conf;
  private Version version;
  private Version singleStoreVersion;

  /**
   * Constructor.
   *
   * @param connection connection
   * @param conf configuration
   */
  public DatabaseMetaData(com.singlestore.jdbc.Connection connection, Configuration conf) {
    this.connection = connection;
    this.conf = conf;
    this.version = null;
    this.singleStoreVersion = null;
  }

  private static String DataTypeClause(Configuration conf) {
    String upperCaseWithoutPersisted =
        "UCASE(IF( UCASE(COLUMN_TYPE) LIKE '%PERSISTED%', SUBSTRING(COLUMN_TYPE,"
            + " 10 + LOCATE('PERSISTED', UCASE(COLUMN_TYPE))), COLUMN_TYPE))";
    String upperCaseWithoutSize =
        " UCASE(IF( "
            + upperCaseWithoutPersisted
            + " LIKE '%(%)%', CONCAT(SUBSTRING( "
            + upperCaseWithoutPersisted
            + ",1, LOCATE('(',"
            + upperCaseWithoutPersisted
            + ") - 1 ), SUBSTRING("
            + upperCaseWithoutPersisted
            + " ,1+locate(')', "
            + upperCaseWithoutPersisted
            + "))), "
            + upperCaseWithoutPersisted
            + "))";

    if (conf.tinyInt1isBit()) {
      upperCaseWithoutSize =
          " IF(COLUMN_TYPE like 'tinyint%', '"
              + (conf.transformedBitIsBoolean() ? "BOOLEAN" : "BIT")
              + "', "
              + upperCaseWithoutSize
              + ")";
    }

    if (!conf.yearIsDateType()) {
      return " IF(c.COLUMN_TYPE IN ('year(2)', 'year(4)'), 'SMALLINT', "
          + upperCaseWithoutSize
          + ")";
    }

    return upperCaseWithoutSize;
  }

  private static String DateTimeSizeClause(String fullTypeColumnName) {
    return "  WHEN 'time' THEN "
        + "IF("
        + fullTypeColumnName
        + " like 'time(6)%',"
        + "17"
        + ","
        + "10"
        + ")"
        + "  WHEN 'date' THEN 10"
        + "  WHEN 'datetime' THEN "
        + "IF("
        + fullTypeColumnName
        + " like 'datetime(6)%',"
        + "26"
        + ","
        + "19"
        + ")"
        + "  WHEN 'timestamp' THEN "
        + "IF("
        + fullTypeColumnName
        + " like 'timestamp(6)%',"
        + "26"
        + ","
        + "19"
        + ")";
  }

  private static String DateTimeScaleClause(String fullTypeColumnName) {
    return "  WHEN 'time' THEN "
        + "IF("
        + fullTypeColumnName
        + " like 'time(6)%',"
        + "6"
        + ","
        + "0"
        + ")"
        + "  WHEN 'datetime' THEN "
        + "IF("
        + fullTypeColumnName
        + " like 'datetime(6)%',"
        + "6"
        + ","
        + "0"
        + ")"
        + "  WHEN 'timestamp' THEN "
        + "IF("
        + fullTypeColumnName
        + " like 'timestamp(6)%',"
        + "6"
        + ","
        + "0"
        + ")";
  }

  public Version getVersion() throws java.sql.SQLException {
    return conf.useMysqlVersion() ? this.getMySQLVersion() : this.getSingleStoreVersion();
  }

  /**
   * This function is to return the version of S2
   *
   * @return S2 version object
   * @throws java.sql.SQLException
   */
  public Version getSingleStoreVersion() throws java.sql.SQLException {
    if (this.singleStoreVersion == null) {
      String sql = "SELECT @@memsql_version;";
      ResultSet rs = executeQuery(sql);
      rs.next();
      this.singleStoreVersion = new Version(rs.getString(1));
    }
    return this.singleStoreVersion;
  }

  /**
   * This function is to return the version of MySQL
   *
   * @return MySQL Version Object
   * @throws java.sql.SQLException
   */
  private Version getMySQLVersion() throws java.sql.SQLException {
    if (this.version == null) {
      String sql = "SELECT @@version;";
      ResultSet rs = executeQuery(sql);
      rs.next();

      this.version = new Version(rs.getString(1));
    }
    return this.version;
  }

  private ResultSet executeQuery(String sql) throws SQLException {
    Statement stmt = connection.createStatement();
    Result rs = (Result) stmt.executeQuery(sql);
    rs.setStatement(null); // bypass Hibernate statement tracking (CONJ-49)
    rs.useAliasAsName();
    return rs;
  }

  private String returnTypeClause() {
    return " CASE PARAMETER_MODE "
        + "  WHEN 'IN' THEN "
        + functionColumnIn
        + "  WHEN 'OUT' THEN "
        + functionColumnOut
        + "  WHEN 'INOUT' THEN "
        + functionColumnInOut
        + "  ELSE "
        + functionReturn
        + " END";
  }

  private String parameterClause(
      String catColumn, String columnName, String columnType, String ordinal) {
    return "SELECT "
        + catColumn
        + " `FUNCTION_CAT`, NULL `FUNCTION_SCHEM`, SPECIFIC_NAME FUNCTION_NAME,"
        + columnName
        + " COLUMN_NAME, "
        + columnType
        + " COLUMN_TYPE,"
        + dataTypeClause("DTD_IDENTIFIER", "")
        + " DATA_TYPE,"
        + "DATA_TYPE TYPE_NAME,NUMERIC_PRECISION `PRECISION`,CHARACTER_MAXIMUM_LENGTH LENGTH,"
        + "CASE DATA_TYPE "
        + DateTimeScaleClause("DTD_IDENTIFIER")
        + " ELSE NUMERIC_SCALE END `SCALE`,10 RADIX,"
        + procedureNullableUnknown
        + " NULLABLE,NULL REMARKS,"
        + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH ,"
        + ordinal
        + " ORDINAL_POSITION, '' IS_NULLABLE, SPECIFIC_NAME ";
  }

  /**
   * Escape String.
   *
   * @param value value to escape
   * @param noBackslashEscapes must backslash be escaped
   * @return escaped string.
   */
  public static String escapeString(String value, boolean noBackslashEscapes) {
    if (noBackslashEscapes) {
      return value.replace("'", "''");
    }
    return value
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\0", "\\0")
        .replace("\"", "\\\"");
  }

  /**
   * Retrieves a description of the primary key columns that are referenced by the given table's
   * foreign key columns (the primary keys imported by a table). They are ordered by PKTABLE_CAT,
   * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
   *
   * <p>Each primary key column description has the following columns:
   *
   * <OL>
   *   <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog being imported (may be
   *       <code>null</code>)
   *   <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema being imported (may be
   *       <code>null</code>)
   *   <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name being imported
   *   <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name being imported
   *   <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
   *   <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>
   *       )
   *   <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
   *   <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
   *   <LI><B>KEY_SEQ</B> short {@code =>} sequence number within a foreign key( a value of 1
   *       represents the first column of the foreign key, a value of 2 would represent the second
   *       column within the foreign key).
   *   <LI><B>UPDATE_RULE</B> short {@code =>} What happens to a foreign key when the primary key is
   *       updated:
   *       <UL>
   *         <LI>importedNoAction - do not allow update of primary key if it has been imported
   *         <LI>importedKeyCascade - change imported key to agree with primary key update
   *         <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
   *             has been updated
   *         <LI>importedKeySetDefault - change imported key to default values if its primary key
   *             has been updated
   *         <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
   *       </UL>
   *   <LI><B>DELETE_RULE</B> short {@code =>} What happens to the foreign key when primary is
   *       deleted.
   *       <UL>
   *         <LI>importedKeyNoAction - do not allow delete of primary key if it has been imported
   *         <LI>importedKeyCascade - delete rows that import a deleted key
   *         <LI>importedKeySetNull - change imported key to NULL if its primary key has been
   *             deleted
   *         <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
   *         <LI>importedKeySetDefault - change imported key to default if its primary key has been
   *             deleted
   *       </UL>
   *   <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
   *   <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
   *   <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key constraints be
   *       deferred until commit
   *       <UL>
   *         <LI>importedKeyInitiallyDeferred - see SQL92 for definition
   *         <LI>importedKeyInitiallyImmediate - see SQL92 for definition
   *         <LI>importedKeyNotDeferrable - see SQL92 for definition
   *       </UL>
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in the database
   * @return <code>ResultSet</code> - each row is a primary key column description
   * @throws SQLException if a database access error occurs
   * @see #getExportedKeys
   */
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "SingleStore does not support foreign keys and referential integrity");
  }

  private String dataTypeClause(String fullTypeColumnName, String tableAlias) {
    return " CASE "
        + tableAlias
        + "DATA_TYPE"
        + " WHEN 'bit' THEN "
        + Types.BIT
        + " WHEN 'tinyblob' THEN "
        + Types.VARBINARY
        + " WHEN 'mediumblob' THEN "
        + Types.LONGVARBINARY
        + " WHEN 'longblob' THEN "
        + Types.LONGVARBINARY
        + " WHEN 'blob' THEN "
        + Types.LONGVARBINARY
        + " WHEN 'tinytext' THEN "
        + " IF( "
        + tableAlias
        + "CHARACTER_SET_NAME LIKE 'binary', "
        + Types.VARBINARY
        + ", "
        + Types.VARCHAR
        + ") WHEN 'mediumtext' THEN "
        + " IF( "
        + tableAlias
        + "CHARACTER_SET_NAME LIKE 'binary', "
        + Types.LONGVARBINARY
        + ", "
        + Types.LONGVARCHAR
        + ") WHEN 'longtext' THEN "
        + " IF( "
        + tableAlias
        + "CHARACTER_SET_NAME LIKE 'binary', "
        + Types.LONGVARBINARY
        + ", "
        + Types.LONGVARCHAR
        + ") WHEN 'text' THEN "
        + " IF( "
        + tableAlias
        + "CHARACTER_SET_NAME LIKE 'binary', "
        + Types.LONGVARBINARY
        + ", "
        + Types.LONGVARCHAR
        + ") WHEN 'json' THEN "
        + Types.LONGVARCHAR
        + " WHEN 'date' THEN "
        + Types.DATE
        + " WHEN 'datetime' THEN "
        + Types.TIMESTAMP
        + " WHEN 'decimal' THEN "
        + Types.DECIMAL
        + " WHEN 'double' THEN "
        + Types.DOUBLE
        + " WHEN 'enum' THEN "
        + Types.VARCHAR
        + " WHEN 'float' THEN "
        + Types.REAL
        + " WHEN 'int' THEN IF( "
        + tableAlias
        + fullTypeColumnName
        + " like '%unsigned%', "
        + Types.INTEGER
        + ","
        + Types.INTEGER
        + ")"
        + " WHEN 'bigint' THEN "
        + Types.BIGINT
        + " WHEN 'mediumint' THEN "
        + Types.INTEGER
        + " WHEN 'null' THEN "
        + Types.NULL
        + " WHEN 'set' THEN "
        + Types.VARCHAR
        + " WHEN 'smallint' THEN IF( "
        + tableAlias
        + fullTypeColumnName
        + " like '%unsigned%', "
        + Types.SMALLINT
        + ","
        + Types.SMALLINT
        + ")"
        + " WHEN 'varchar' THEN "
        + " IF( "
        + tableAlias
        + "CHARACTER_SET_NAME LIKE 'binary', "
        + Types.VARBINARY
        + ", "
        + Types.VARCHAR
        + ") WHEN 'varbinary' THEN "
        + Types.VARBINARY
        + " WHEN 'char' THEN "
        + Types.CHAR
        + " WHEN 'binary' THEN "
        + Types.BINARY
        + " WHEN 'time' THEN "
        + Types.TIME
        + " WHEN 'timestamp' THEN "
        + Types.TIMESTAMP
        + " WHEN 'tinyint' THEN "
        + (conf.tinyInt1isBit()
            ? "IF("
                + tableAlias
                + fullTypeColumnName
                + " like 'tinyint%',"
                + (conf.transformedBitIsBoolean() ? Types.BOOLEAN : Types.BIT)
                + ","
                + Types.TINYINT
                + ") "
            : Types.TINYINT)
        + " WHEN 'year' THEN "
        + (conf.yearIsDateType() ? Types.DATE : Types.SMALLINT)
        + " ELSE "
        + Types.OTHER
        + " END ";
  }

  private String escapeQuote(String value) {
    return "'"
        + escapeString(
            value,
            (connection.getContext().getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0)
        + "'";
  }

  /**
   * Generate part of the information schema query that restricts catalog names In the driver,
   * catalogs is the equivalent to SingleStore schemas.
   *
   * @param columnName - column name in the information schema table
   * @param catalog - catalog name. This driver does not (always) follow JDBC standard for following
   *     special values, due to ConnectorJ compatibility 1. empty string ("") - matches current
   *     catalog (i.e database). JDBC standard says only tables without catalog should be returned -
   *     such tables do not exist in SingleStore. If there is no current catalog, then empty string
   *     matches any catalog. 2. null - if nullCatalogMeansCurrent=true (which is the default), then
   *     the handling is the same as for "" . i.e return current catalog.JDBC-conforming way would
   *     be to match any catalog with null parameter. This can be switched with
   *     nullCatalogMeansCurrent=false in the connection URL.
   * @return part of SQL query ,that restricts search for the catalog.
   */
  private String catalogCond(String columnName, String catalog) {
    if (catalog == null || catalog.isEmpty()) {
      return "(ISNULL(database()) OR (" + columnName + " = database()))";
    }
    return "(" + columnName + " = " + escapeQuote(catalog) + ")";
  }

  // Helper to generate  information schema queries with "like" or "equals" condition (typically  on
  // table name)
  private String patternCond(String columnName, String tableName) {
    if (tableName == null) {
      return "";
    }
    String predicate =
        (tableName.indexOf('%') == -1 && tableName.indexOf('_') == -1) ? "=" : "LIKE";
    return " AND "
        + columnName
        + " "
        + predicate
        + " '"
        + escapeString(
            tableName,
            (connection.getContext().getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0)
        + "' ";
  }

  /**
   * Retrieves a description of the given table's primary key columns. They are ordered by
   * COLUMN_NAME.
   *
   * <p>Each primary key column description has the following columns:
   *
   * <OL>
   *   <li><B>TABLE_CAT</B> String {@code =>} table catalog
   *   <li><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <li><B>TABLE_NAME</B> String {@code =>} table name
   *   <li><B>COLUMN_NAME</B> String {@code =>} column name
   *   <li><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value of 1
   *       represents the first column of the primary key, a value of 2 would represent the second
   *       column within the primary key).
   *   <li><B>PK_NAME</B> String {@code =>} primary key name
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in the database
   * @return <code>ResultSet</code> - each row is a primary key column description
   * @throws SQLException if a database access error occurs
   */
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    // MySQL 8 now use 'PRI' in place of 'pri'
    String sql =
        "SELECT DISTINCT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX KEY_SEQ, INDEX_NAME PK_NAME "
            + " FROM INFORMATION_SCHEMA.STATISTICS "
            + "WHERE INDEX_NAME='PRIMARY' "
            + " AND "
            + catalogCond("TABLE_SCHEMA", catalog)
            + patternCond("TABLE_NAME", table)
            + " ORDER BY COLUMN_NAME";

    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the tables available in the given catalog. Only table descriptions
   * matching the catalog, schema, table name and type criteria are returned. They are ordered by
   * <code>TABLE_TYPE</code>, <code>TABLE_CAT</code>, <code>TABLE_SCHEM</code> and <code>TABLE_NAME
   * </code>. Each table description has the following columns:
   *
   * <OL>
   *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
   *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <LI><B>TABLE_NAME</B> String {@code =>} table name
   *   <LI><B>TABLE_TYPE</B> String {@code =>} table type. Typical types are "TABLE", "VIEW",
   *       "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
   *   <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table
   *   <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be <code>null</code>)
   *   <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be <code>null</code>)
   *   <LI><B>TYPE_NAME</B> String {@code =>} type name (may be <code>null</code>)
   *   <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated "identifier"
   *       column of a typed table (may be <code>null</code>)
   *   <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in SELF_REFERENCING_COL_NAME
   *       are created. Values are "SYSTEM", "USER", "DERIVED". (may be <code>null</code>)
   * </OL>
   *
   * <p><B>Note:</B> Some databases may not return information for all tables.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
   *     database
   * @param types a list of table types, which must be from the list of table types returned from
   *     {@link #getTableTypes},to include; <code>null</code> returns all types
   * @return <code>ResultSet</code> - each row is a table description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {

    // A FLAGS column was introduced to INFORMATION_SCHEMA.TABLES to distinguish between
    // TVFs and other table types in order to exclude them from SHOW TABLES.
    // If we cannot use FLAGS, we have to join ROUTINES to exclude TVFs
    boolean canUseFlags = getSingleStoreVersion().versionGreaterOrEqual(7, 8, 1);

    StringBuilder sql =
        new StringBuilder(
            "SELECT TABLE_SCHEMA TABLE_CAT, NULL  TABLE_SCHEM,  TABLE_NAME,"
                + " (CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'"
                + " WHEN TABLE_TYPE = 'SYSTEM VIEW' THEN 'VIEW' ELSE TABLE_TYPE END) AS TABLE_TYPE,"
                + " TABLE_COMMENT REMARKS, NULL TYPE_CAT, NULL TYPE_SCHEM, NULL TYPE_NAME, NULL SELF_REFERENCING_COL_NAME, "
                + " NULL REF_GENERATION"
                + " FROM INFORMATION_SCHEMA.TABLES "
                + (canUseFlags
                    ? ""
                    : "LEFT JOIN INFORMATION_SCHEMA.ROUTINES ON "
                        + "(TABLE_NAME=ROUTINE_NAME AND TABLE_SCHEMA=ROUTINE_SCHEMA)")
                + " WHERE "
                + (canUseFlags
                    ? "CAST(FLAGS AS UNSIGNED INTEGER) & 1 = 0 AND "
                    : "ROUTINE_NAME IS NULL AND ")
                + catalogCond("TABLE_SCHEMA", catalog)
                + patternCond("TABLE_NAME", tableNamePattern));

    if (types != null && types.length > 0) {
      boolean mustAddType = false;
      StringBuilder sqlType = new StringBuilder(" AND TABLE_TYPE IN (");
      for (int i = 0; i < types.length; i++) {
        if (mustAddType) sqlType.append(",");
        mustAddType = true;
        if (types[i] == null) {
          mustAddType = false;
          continue;
        }
        if ("information_schema".equalsIgnoreCase(catalog) && "VIEW".equals(types[i])) {
          sqlType.append("'SYSTEM VIEW'");
        } else {
          String type = "TABLE".equals(types[i]) ? "'BASE TABLE'" : escapeQuote(types[i]);
          sqlType.append(type);
        }
      }
      sqlType.append(")");
      if (mustAddType) sql.append(sqlType);
    }

    sql.append(" ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");

    return executeQuery(sql.toString());
  }

  /**
   * Retrieves a description of table columns available in the specified catalog.
   *
   * <p>Only column descriptions matching the catalog, schema, table and column name criteria are
   * returned. They are ordered by <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>, <code>TABLE_NAME
   * </code>, and <code>ORDINAL_POSITION</code>.
   *
   * <p>Each column description has the following columns:
   *
   * <OL>
   *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
   *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <LI><B>TABLE_NAME</B> String {@code =>} table name
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   *   <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name, for a UDT the type
   *       name is fully qualified
   *   <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
   *   <LI><B>BUFFER_LENGTH</B> is not used.
   *   <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
   *       for data types where DECIMAL_DIGITS is not applicable.
   *   <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
   *   <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
   *       <UL>
   *         <LI>columnNoNulls - might not allow <code>NULL</code> values
   *         <LI>columnNullable - definitely allows <code>NULL</code> values
   *         <LI>columnNullableUnknown - nullability unknown
   *       </UL>
   *   <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
   *   <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be
   *       interpreted as a string when the value is enclosed in single quotes (may be <code>null
   * </code>)
   *   <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
   *   <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
   *   <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in the
   *       column
   *   <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table (starting at 1)
   *   <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
   *       a column.
   *       <UL>
   *         <LI>YES --- if the column can include NULLs
   *         <LI>NO --- if the column cannot include NULLs
   *         <LI>empty string --- if the nullability for the column is unknown
   *       </UL>
   *   <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope of a reference
   *       attribute (<code>null</code> if DATA_TYPE isn't REF)
   *   <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope of a reference
   *       attribute (<code>null</code> if the DATA_TYPE isn't REF)
   *   <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope of a reference
   *       attribute (<code>null</code> if the DATA_TYPE isn't REF)
   *   <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
   *       Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE isn't DISTINCT or
   *       user-generated REF)
   *   <LI><B>IS_AUTOINCREMENT</B> String {@code =>} Indicates whether this column is auto
   *       incremented
   *       <UL>
   *         <LI>YES --- if the column is auto incremented
   *         <LI>NO --- if the column is not auto incremented
   *         <LI>empty string --- if it cannot be determined whether the column is auto incremented
   *       </UL>
   *   <LI><B>IS_GENERATEDCOLUMN</B> String {@code =>} Indicates whether this is a generated column
   *       <UL>
   *         <LI>YES --- if this a generated column
   *         <LI>NO --- if this not a generated column
   *         <LI>empty string --- if it cannot be determined whether this is a generated column
   *       </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column specifies the column size for the given column. For numeric data,
   * this is the maximum precision. For character data, this is the length in characters. For
   * datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the
   * length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data
   * types where the column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
   *     database
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in
   *     the database
   * @return <code>ResultSet</code> - each row is a column description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    String fullTypeColumnName = "COLUMN_TYPE";

    // A FLAGS column was introduced to information_schema.TABLES to distinguish between
    // TVFs and other table types in order to exclude them from SHOW TABLES.
    // If the flags are available, join information_schema.TABLES and use them.
    // If not, we have to join information_schema.ROUTINES to exclude TVFs.
    // This is because some users may not have access rights to information_schema.ROUTINES
    boolean canUseFlags = getSingleStoreVersion().versionGreaterOrEqual(7, 8, 1);

    String sql =
        "SELECT c.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, c.TABLE_NAME TABLE_NAME, c.COLUMN_NAME COLUMN_NAME,"
            + dataTypeClause(fullTypeColumnName, "c.")
            + " DATA_TYPE,"
            + DataTypeClause(conf)
            + " TYPE_NAME, "
            + " CASE c.DATA_TYPE"
            + DateTimeSizeClause(fullTypeColumnName)
            + (conf.yearIsDateType() ? "" : " WHEN 'year' THEN 5")
            + "  ELSE "
            + "  IF(c.NUMERIC_PRECISION IS NULL, LEAST(c.CHARACTER_MAXIMUM_LENGTH,"
            + Integer.MAX_VALUE
            + "), c.NUMERIC_PRECISION) "
            + " END"
            + " COLUMN_SIZE, 65535 BUFFER_LENGTH, "
            + " CONVERT (CASE c.DATA_TYPE"
            + " WHEN 'year' THEN "
            + (conf.yearIsDateType() ? "c.NUMERIC_SCALE" : "0")
            + " WHEN 'tinyint' THEN "
            + (conf.tinyInt1isBit() ? "0" : "c.NUMERIC_SCALE")
            + " ELSE c.NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS,"
            + " 10 NUM_PREC_RADIX, IF(c.IS_NULLABLE = 'yes',1,0) NULLABLE,c.COLUMN_COMMENT REMARKS,"
            + " c.COLUMN_DEFAULT COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB,  "
            + " LEAST(c.CHARACTER_OCTET_LENGTH,"
            + Integer.MAX_VALUE
            + ") CHAR_OCTET_LENGTH,"
            + " c.ORDINAL_POSITION ORDINAL_POSITION, c.IS_NULLABLE IS_NULLABLE, NULL SCOPE_CATALOG, NULL SCOPE_SCHEMA, NULL SCOPE_TABLE, NULL SOURCE_DATA_TYPE,"
            + " IF(c.EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT, "
            + " IF(c.EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED', 'COMPUTED') ,'YES','NO') IS_GENERATEDCOLUMN "
            + " FROM INFORMATION_SCHEMA.COLUMNS c "
            + (canUseFlags
                ? "LEFT JOIN INFORMATION_SCHEMA.TABLES AS t ON "
                    + "(c.TABLE_NAME=t.TABLE_NAME AND c.TABLE_SCHEMA=t.TABLE_SCHEMA)"
                : "LEFT JOIN INFORMATION_SCHEMA.ROUTINES AS r ON "
                    + "(c.TABLE_NAME=r.ROUTINE_NAME AND c.TABLE_SCHEMA=r.ROUTINE_SCHEMA)")
            + " WHERE "
            + (canUseFlags
                ? "CAST(t.FLAGS AS UNSIGNED INTEGER) & 1 = 0 AND "
                : "r.ROUTINE_NAME IS NULL AND ")
            + catalogCond("c.TABLE_SCHEMA", catalog)
            + patternCond("c.TABLE_NAME", tableNamePattern)
            + patternCond("c.COLUMN_NAME", columnNamePattern)
            + " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the foreign key columns that reference the given table's primary key
   * columns (the foreign keys exported by a table). They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
   * FKTABLE_NAME, and KEY_SEQ.
   *
   * <p>Each foreign key column description has the following columns:
   *
   * <OL>
   *   <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog (may be <code>null</code>)
   *   <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema (may be <code>null</code>
   *       )
   *   <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name
   *   <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name
   *   <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
   *       being exported (may be <code>null</code>)
   *   <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>
   *       ) being exported (may be <code>null</code>)
   *   <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name being exported
   *   <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name being exported
   *   <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value of 1
   *       represents the first column of the foreign key, a value of 2 would represent the second
   *       column within the foreign key).
   *   <LI><B>UPDATE_RULE</B> short {@code =>} What happens to foreign key when primary is updated:
   *       <UL>
   *         <LI>importedNoAction - do not allow update of primary key if it has been imported
   *         <LI>importedKeyCascade - change imported key to agree with primary key update
   *         <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
   *             has been updated
   *         <LI>importedKeySetDefault - change imported key to default values if its primary key
   *             has been updated
   *         <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
   *       </UL>
   *   <LI><B>DELETE_RULE</B> short {@code =>} What happens to the foreign key when primary is
   *       deleted.
   *       <UL>
   *         <LI>importedKeyNoAction - do not allow delete of primary key if it has been imported
   *         <LI>importedKeyCascade - delete rows that import a deleted key
   *         <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
   *             has been deleted
   *         <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
   *         <LI>importedKeySetDefault - change imported key to default if its primary key has been
   *             deleted
   *       </UL>
   *   <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
   *   <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
   *   <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key constraints be
   *       deferred until commit
   *       <UL>
   *         <LI>importedKeyInitiallyDeferred - see SQL92 for definition
   *         <LI>importedKeyInitiallyImmediate - see SQL92 for definition
   *         <LI>importedKeyNotDeferrable - see SQL92 for definition
   *       </UL>
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in this database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in this database
   * @return a <code>ResultSet</code> object in which each row is a foreign key column description
   * @throws SQLException if a database access error occurs
   * @see #getImportedKeys
   */
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "SingleStore does not support foreign keys and referential integrity");
  }

  /**
   * Retrieves a description of a table's optimal set of columns that uniquely identifies a row.
   * They are ordered by SCOPE.
   *
   * <p>Each column description has the following columns:
   *
   * <OL>
   *   <LI><B>SCOPE</B> short {@code =>} actual scope of result
   *       <UL>
   *         <LI>bestRowTemporary - very temporary, while using row
   *         <LI>bestRowTransaction - valid for remainder of current transaction
   *         <LI>bestRowSession - valid for remainder of current session
   *       </UL>
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
   *   <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name, for a UDT the type
   *       name is fully qualified
   *   <LI><B>COLUMN_SIZE</B> int {@code =>} precision
   *   <LI><B>BUFFER_LENGTH</B> int {@code =>} not used
   *   <LI><B>DECIMAL_DIGITS</B> short {@code =>} scale - Null is returned for data types where
   *       DECIMAL_DIGITS is not applicable.
   *   <LI><B>PSEUDO_COLUMN</B> short {@code =>} is this a pseudo column like an Oracle ROWID
   *       <UL>
   *         <LI>bestRowUnknown - may or may not be pseudo column
   *         <LI>bestRowNotPseudo - is NOT a pseudo column
   *         <LI>bestRowPseudo - is a pseudo column
   *       </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column represents the specified column size for the given column. For
   * numeric data, this is the maximum precision. For character data, this is the length in
   * characters. For datetime datatypes, this is the length in characters of the String
   * representation (assuming the maximum allowed precision of the fractional seconds component).
   * For binary data, this is the length in bytes. For the ROWID datatype, this is the length in
   * bytes. Null is returned for data types where the column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in the database
   * @param scope the scope of interest; use same values as SCOPE
   * @param nullable include columns that are nullable.
   * @return <code>ResultSet</code> - each row is a column description
   * @throws SQLException if a database access error occurs
   */
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, final boolean nullable)
      throws SQLException {

    if (table == null) {
      throw new SQLException("'table' parameter cannot be null in getBestRowIdentifier()");
    }

    String sql =
        "SELECT "
            + bestRowSession
            + " SCOPE, COLUMN_NAME,"
            + dataTypeClause("COLUMN_TYPE", "")
            + " DATA_TYPE, DATA_TYPE TYPE_NAME,"
            + " IF(NUMERIC_PRECISION IS NULL, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) COLUMN_SIZE, 0 BUFFER_LENGTH,"
            + " NUMERIC_SCALE DECIMAL_DIGITS,"
            + bestRowNotPseudo
            + " PSEUDO_COLUMN"
            + " FROM INFORMATION_SCHEMA.COLUMNS"
            + " WHERE (COLUMN_KEY  = 'PRI'"
            + " OR (COLUMN_KEY = 'UNI' AND NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_KEY = "
            + "'PRI' AND "
            + catalogCond("TABLE_SCHEMA", catalog)
            + " AND TABLE_NAME = "
            + escapeQuote(table)
            + " ))) AND "
            + catalogCond("TABLE_SCHEMA", catalog)
            + " AND TABLE_NAME = "
            + escapeQuote(table)
            + (nullable ? "" : " AND IS_NULLABLE = 'NO'");

    return executeQuery(sql);
  }

  public boolean generatedKeyAlwaysReturned() {
    return true;
  }

  /**
   * Retrieves a description of the pseudo or hidden columns available in a given table within the
   * specified catalog and schema. Pseudo or hidden columns may not always be stored within a table
   * and are not visible in a ResultSet unless they are specified in the query's outermost SELECT
   * list. Pseudo or hidden columns may not necessarily be able to be modified. If there are no
   * pseudo or hidden columns, an empty ResultSet is returned.
   *
   * <p>Only column descriptions matching the catalog, schema, table and column name criteria are
   * returned. They are ordered by <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>, <code>TABLE_NAME
   * </code> and <code>COLUMN_NAME</code>.
   *
   * <p>Each column description has the following columns:
   *
   * <OL>
   *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
   *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <LI><B>TABLE_NAME</B> String {@code =>} table name
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   *   <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
   *   <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
   *       for data types where DECIMAL_DIGITS is not applicable.
   *   <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
   *   <LI><B>COLUMN_USAGE</B> String {@code =>} The allowed usage for the column. The value
   *       returned will correspond to the enum name returned by PseudoColumnUsage.name()
   *   <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
   *   <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in the
   *       column
   *   <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
   *       a column.
   *       <UL>
   *         <LI>YES --- if the column can include NULLs
   *         <LI>NO --- if the column cannot include NULLs
   *         <LI>empty string --- if the nullability for the column is unknown
   *       </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column specifies the column size for the given column. For numeric data,
   * this is the maximum precision. For character data, this is the length in characters. For
   * datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the
   * length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data
   * types where the column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
   *     database
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in
   *     the database
   * @return <code>ResultSet</code> - each row is a column description
   * @throws SQLException if a database access error occurs
   * @see PseudoColumnUsage
   * @since 1.7
   */
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    return connection
        .createStatement()
        .executeQuery(
            "SELECT ' ' TABLE_CAT, ' ' TABLE_SCHEM, ' ' TABLE_NAME, ' ' COLUMN_NAME, 0 DATA_TYPE, 0 COLUMN_SIZE, "
                + "0 DECIMAL_DIGITS, 10 NUM_PREC_RADIX, ' ' COLUMN_USAGE,  ' ' REMARKS, 0 CHAR_OCTET_LENGTH, "
                + "'YES' IS_NULLABLE FROM DUAL WHERE 1=0");
  }

  public boolean allProceduresAreCallable() {
    return true;
  }

  public boolean allTablesAreSelectable() {
    return true;
  }

  @Override
  public String getURL() {
    return conf.initialUrl();
  }

  public String getUserName() {
    return conf.user();
  }

  public boolean isReadOnly() throws SQLException {
    java.sql.Statement st = connection.createStatement();
    ResultSet rs = st.executeQuery("SELECT @@READ_ONLY");
    rs.next();
    return rs.getInt(1) == 1;
  }

  public boolean nullsAreSortedHigh() {
    return false;
  }

  public boolean nullsAreSortedLow() {
    return true;
  }

  public boolean nullsAreSortedAtStart() {
    return false;
  }

  public boolean nullsAreSortedAtEnd() {
    return true;
  }

  /**
   * Return Server type. MySQL or SingleStore. MySQL can be forced for compatibility with option
   * "useMysqlMetadata"
   *
   * @return server type
   */
  public String getDatabaseProductName() {
    return "SingleStore";
  }

  public String getDatabaseProductVersion() throws SQLException {
    return getSingleStoreVersion().getVersion();
  }

  public String getDriverName() {
    return DRIVER_NAME;
  }

  public String getDriverVersion() {
    return VersionFactory.getInstance().getVersion();
  }

  public int getDriverMajorVersion() {
    return VersionFactory.getInstance().getMajorVersion();
  }

  public int getDriverMinorVersion() {
    return VersionFactory.getInstance().getMinorVersion();
  }

  public boolean usesLocalFiles() {
    return false;
  }

  public boolean usesLocalFilePerTable() {
    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return (connection.getLowercaseTableNames() == 0);
  }

  public boolean storesUpperCaseIdentifiers() {
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

  public boolean storesUpperCaseQuotedIdentifiers() {
    return storesUpperCaseIdentifiers();
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return storesLowerCaseIdentifiers();
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return storesMixedCaseIdentifiers();
  }

  public String getIdentifierQuoteString() {
    return "`";
  }

  /**
   * Retrieves a comma-separated list of all of this database's SQL keywords that are NOT also
   * SQL:2003 keywords.
   *
   * @return the list of this database's keywords that are not also SQL:2003 keywords
   */
  @Override
  public String getSQLKeywords() {
    return "ACCESSIBLE,ANALYZE,ASENSITIVE,BEFORE,BIGINT,BINARY,BLOB,CALL,CHANGE,CONDITION,DATABASE,DATABASES,"
        + "DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DETERMINISTIC,DISTINCTROW,DIV,DUAL,EACH,"
        + "ELSEIF,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERAL,HIGH_PRIORITY,"
        + "HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,IGNORE_SERVER_IDS,INDEX,INFILE,INOUT,INT1,INT2,"
        + "INT3,INT4,INT8,ITERATE,KEY,KEYS,KILL,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,"
        + "LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_HEARTBEAT_PERIOD,MASTER_SSL_VERIFY_SERVER_CERT,"
        + "MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,"
        + "NO_WRITE_TO_BINLOG,OPTIMIZE,OPTIONALLY,OUT,OUTFILE,PURGE,RANGE,READ_WRITE,READS,REGEXP,RELEASE,"
        + "RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RETURN,RLIKE,SCHEMAS,SECOND_MICROSECOND,SENSITIVE,"
        + "SEPARATOR,SHOW,SIGNAL,SLOW,SPATIAL,SPECIFIC,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,"
        + "SQLEXCEPTION,SSL,STARTING,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,TRIGGER,UNDO,UNLOCK,"
        + "UNSIGNED,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,WHILE,XOR,YEAR_MONTH,ZEROFILL";
  }

  /**
   * List of numeric functions.
   *
   * @return List of numeric functions.
   */
  @Override
  public String getNumericFunctions() {
    return "DIV,ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,CEILING,CONV,COS,COT,CRC32,DEGREES,EXP,FLOOR,GREATEST,LEAST,LN,LOG,"
        + "LOG10,LOG2,MOD,OCT,PI,POW,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE";
  }

  /**
   * List of string functions.
   *
   * @return List of string functions.
   */
  @Override
  public String getStringFunctions() {
    return "ASCII,BIN,BIT_LENGTH,CAST,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONVERT,ELT,EXPORT_SET,"
        + "EXTRACTVALUE,FIELD,FIND_IN_SET,FORMAT,FROM_BASE64,HEX,INSTR,LCASE,LEFT,LENGTH,LIKE,LOAD_FILE,LOCATE,"
        + "LOWER,LPAD,LTRIM,MAKE_SET,MATCH AGAINST,MID,NOT LIKE,NOT REGEXP,OCTET_LENGTH,ORD,POSITION,QUOTE,"
        + "REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SOUNDS LIKE,SPACE,STRCMP,SUBSTR,SUBSTRING,"
        + "SUBSTRING_INDEX,TO_BASE64,TRIM,UCASE,UNHEX,UPDATEXML,UPPER,WEIGHT_STRING";
  }

  /**
   * List of system functions.
   *
   * @return List of system functions.
   */
  @Override
  public String getSystemFunctions() {
    return "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION";
  }

  /**
   * List of time and date functions.
   *
   * @return List of time and date functions.
   */
  @Override
  public String getTimeDateFunctions() {
    return "ADDDATE,ADDTIME,CONVERT_TZ,CURDATE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURTIME,DATEDIFF,"
        + "DATE_ADD,DATE_FORMAT,DATE_SUB,DAY,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,EXTRACT,FROM_DAYS,"
        + "FROM_UNIXTIME,GET_FORMAT,HOUR,LAST_DAY,LOCALTIME,LOCALTIMESTAMP,MAKEDATE,MAKETIME,MICROSECOND,"
        + "MINUTE,MONTH,MONTHNAME,NOW,PERIOD_ADD,PERIOD_DIFF,QUARTER,SECOND,SEC_TO_TIME,STR_TO_DATE,SUBDATE,"
        + "SUBTIME,SYSDATE,TIMEDIFF,TIMESTAMPADD,TIMESTAMPDIFF,TIME_FORMAT,TIME_TO_SEC,TO_DAYS,TO_SECONDS,"
        + "UNIX_TIMESTAMP,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,WEEK,WEEKDAY,WEEKOFYEAR,YEAR,YEARWEEK";
  }

  public String getSearchStringEscape() {
    return "\\";
  }

  public String getExtraNameCharacters() {
    return "#@";
  }

  public boolean supportsAlterTableWithAddColumn() {
    return true;
  }

  public boolean supportsAlterTableWithDropColumn() {
    return true;
  }

  public boolean supportsColumnAliasing() {
    return true;
  }

  public boolean nullPlusNonNullIsNull() {
    return true;
  }

  public boolean supportsConvert() {
    return true;
  }

  /**
   * Retrieves whether this database supports the JDBC scalar function CONVERT for conversions
   * between the JDBC types fromType and toType. The JDBC types are the generic SQL data types
   * defined in java.sql.Types.
   *
   * @param fromType the type to convert from; one of the type codes from the class java.sql.Types
   * @param toType the type to convert to; one of the type codes from the class java.sql.Types
   * @return true if so; false otherwise
   */
  public boolean supportsConvert(int fromType, int toType) {
    switch (fromType) {
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.REAL:
      case Types.FLOAT:
      case Types.DECIMAL:
      case Types.NUMERIC:
      case Types.DOUBLE:
      case Types.BIT:
      case Types.BOOLEAN:
        switch (toType) {
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
          case Types.REAL:
          case Types.FLOAT:
          case Types.DECIMAL:
          case Types.NUMERIC:
          case Types.DOUBLE:
          case Types.BIT:
          case Types.BOOLEAN:
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            return true;
          default:
            return false;
        }

      case Types.BLOB:
        switch (toType) {
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
          case Types.REAL:
          case Types.FLOAT:
          case Types.DECIMAL:
          case Types.NUMERIC:
          case Types.DOUBLE:
          case Types.BIT:
          case Types.BOOLEAN:
            return true;
          default:
            return false;
        }

      case Types.CHAR:
      case Types.CLOB:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        switch (toType) {
          case Types.BIT:
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
          case Types.FLOAT:
          case Types.REAL:
          case Types.DOUBLE:
          case Types.NUMERIC:
          case Types.DECIMAL:
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
          case Types.DATE:
          case Types.TIME:
          case Types.TIMESTAMP:
          case Types.BLOB:
          case Types.CLOB:
          case Types.BOOLEAN:
          case Types.NCHAR:
          case Types.LONGNVARCHAR:
          case Types.NCLOB:
            return true;
          default:
            return false;
        }

      case Types.DATE:
        switch (toType) {
          case Types.DATE:
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            return true;

          default:
            return false;
        }

      case Types.TIME:
        switch (toType) {
          case Types.TIME:
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
            return true;
          default:
            return false;
        }

      case Types.TIMESTAMP:
        switch (toType) {
          case Types.TIMESTAMP:
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
          case Types.TIME:
          case Types.DATE:
            return true;
          default:
            return false;
        }
      default:
        return false;
    }
  }

  public boolean supportsTableCorrelationNames() {
    return true;
  }

  public boolean supportsDifferentTableCorrelationNames() {
    return true;
  }

  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  public boolean supportsOrderByUnrelated() {
    return true;
  }

  public boolean supportsGroupBy() {
    return true;
  }

  public boolean supportsGroupByUnrelated() {
    return true;
  }

  public boolean supportsGroupByBeyondSelect() {
    return true;
  }

  public boolean supportsLikeEscapeClause() {
    return true;
  }

  public boolean supportsMultipleResultSets() {
    return true;
  }

  public boolean supportsMultipleTransactions() {
    return true;
  }

  public boolean supportsNonNullableColumns() {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() {
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() {
    return true;
  }

  @Override
  public boolean supportsANSI92FullSQL() {
    return true;
  }

  public boolean supportsIntegrityEnhancementFacility() {
    return true;
  }

  public boolean supportsOuterJoins() {
    return true;
  }

  public boolean supportsFullOuterJoins() {
    return true;
  }

  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  public String getSchemaTerm() {
    return "schema";
  }

  public String getProcedureTerm() {
    return "procedure";
  }

  public String getCatalogTerm() {
    return "database";
  }

  public boolean isCatalogAtStart() {
    return true;
  }

  public String getCatalogSeparator() {
    return ".";
  }

  public boolean supportsSchemasInDataManipulation() {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() {
    return true;
  }

  public boolean supportsCatalogsInProcedureCalls() {
    return true;
  }

  public boolean supportsCatalogsInTableDefinitions() {
    return true;
  }

  public boolean supportsCatalogsInIndexDefinitions() {
    return true;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return true;
  }

  public boolean supportsPositionedDelete() {
    return false;
  }

  public boolean supportsPositionedUpdate() {
    return false;
  }

  public boolean supportsSelectForUpdate() {
    return true;
  }

  public boolean supportsStoredProcedures() {
    return true;
  }

  public boolean supportsSubqueriesInComparisons() {
    return true;
  }

  public boolean supportsSubqueriesInExists() {
    return true;
  }

  public boolean supportsSubqueriesInIns() {
    return true;
  }

  public boolean supportsSubqueriesInQuantifieds() {
    return true;
  }

  public boolean supportsCorrelatedSubqueries() {
    return true;
  }

  public boolean supportsUnion() {
    return true;
  }

  public boolean supportsUnionAll() {
    return true;
  }

  public boolean supportsOpenCursorsAcrossCommit() {
    return true;
  }

  public boolean supportsOpenCursorsAcrossRollback() {
    return true;
  }

  public boolean supportsOpenStatementsAcrossCommit() {
    return true;
  }

  public boolean supportsOpenStatementsAcrossRollback() {
    return true;
  }

  public int getMaxBinaryLiteralLength() {
    return Integer.MAX_VALUE;
  }

  public int getMaxCharLiteralLength() {
    return Integer.MAX_VALUE;
  }

  public int getMaxColumnNameLength() {
    return 64;
  }

  public int getMaxColumnsInGroupBy() {
    return 64;
  }

  public int getMaxColumnsInIndex() {
    return 16;
  }

  public int getMaxColumnsInOrderBy() {
    return 64;
  }

  public int getMaxColumnsInSelect() {
    return Short.MAX_VALUE;
  }

  public int getMaxColumnsInTable() {
    return 0;
  }

  public int getMaxConnections() {
    return 0;
  }

  public int getMaxCursorNameLength() {
    return 0;
  }

  public int getMaxIndexLength() {
    return 256;
  }

  public int getMaxSchemaNameLength() {
    return 0;
  }

  public int getMaxProcedureNameLength() {
    return 64;
  }

  public int getMaxCatalogNameLength() {
    return 0;
  }

  public int getMaxRowSize() {
    return 0;
  }

  public boolean doesMaxRowSizeIncludeBlobs() {
    return false;
  }

  public int getMaxStatementLength() {
    return 0;
  }

  public int getMaxStatements() {
    return 0;
  }

  public int getMaxTableNameLength() {
    return 64;
  }

  public int getMaxTablesInSelect() {
    return 256;
  }

  public int getMaxUserNameLength() {
    return 0;
  }

  public int getDefaultTransactionIsolation() {
    return java.sql.Connection.TRANSACTION_REPEATABLE_READ;
  }

  /**
   * Retrieves whether this database supports transactions. If not, invoking the method <code>commit
   * </code> is a noop, and the isolation level is <code>TRANSACTION_NONE</code>.
   *
   * @return <code>true</code> if transactions are supported; <code>false</code> otherwise
   */
  public boolean supportsTransactions() {
    return true;
  }

  /* Helper to generate  information schema with "equality" condition (typically on catalog name)
   */

  /**
   * Retrieves whether this database supports the given transaction isolation level.
   *
   * @param level one of the transaction isolation levels defined in <code>java.sql.Connection
   *              </code>
   * @return <code>true</code> if so; <code>false</code> otherwise
   * @see java.sql.Connection
   */
  public boolean supportsTransactionIsolationLevel(int level) {
    switch (level) {
      case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
      case java.sql.Connection.TRANSACTION_READ_COMMITTED:
      case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
      case java.sql.Connection.TRANSACTION_SERIALIZABLE:
        return true;
      default:
        return false;
    }
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return true;
  }

  public boolean supportsDataManipulationTransactionsOnly() {
    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit() {
    return true;
  }

  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  /**
   * Retrieves a description of the stored procedures available in the given catalog. Only procedure
   * descriptions matching the schema and procedure name criteria are returned. They are ordered by
   * <code>PROCEDURE_CAT</code>, <code>PROCEDURE_SCHEM</code>, <code>PROCEDURE_NAME</code> and
   * <code>SPECIFIC_ NAME</code>.
   *
   * <p>Each procedure description has the the following columns:
   *
   * <OL>
   *   <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be <code>null</code>)
   *   <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be <code>null</code>)
   *   <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
   *   <LI>reserved for future use
   *   <LI>reserved for future use
   *   <LI>reserved for future use
   *   <LI><B>REMARKS</B> String {@code =>} explanatory comment on the procedure
   *   <LI><B>PROCEDURE_TYPE</B> short {@code =>} kind of procedure:
   *       <UL>
   *         <LI>procedureResultUnknown - Cannot determine if a return value will be returned
   *         <LI>procedureNoResult - Does not return a return value
   *         <LI>procedureReturnsResult - Returns a return value
   *       </UL>
   *   <LI><B>SPECIFIC_NAME</B> String {@code =>} The name which uniquely identifies this procedure
   *       within its schema.
   * </OL>
   *
   * <p>A user may not have permissions to execute any of the procedures that are returned by <code>
   * getProcedures</code>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param procedureNamePattern a procedure name pattern; must match the procedure name as it is
   *     stored in the database
   * @return <code>ResultSet</code> - each row is a procedure description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {

    String sql =
        "SELECT ROUTINE_SCHEMA PROCEDURE_CAT,NULL PROCEDURE_SCHEM, ROUTINE_NAME PROCEDURE_NAME,"
            + " NULL RESERVED1, NULL RESERVED2, NULL RESERVED3, ROUTINE_COMMENT REMARKS,"
            + " IF( DATA_TYPE = '', "
            + procedureNoResult
            + ", "
            + procedureReturnsResult
            + ") PROCEDURE_TYPE,"
            + " SPECIFIC_NAME "
            + " FROM INFORMATION_SCHEMA.ROUTINES "
            + " WHERE "
            + catalogCond("ROUTINE_SCHEMA", catalog)
            + patternCond("ROUTINE_NAME", procedureNamePattern)
            + " AND ROUTINE_TYPE='PROCEDURE'";
    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the given catalog's stored procedure parameter and result columns.
   *
   * <p>Only descriptions matching the schema, procedure and parameter name criteria are returned.
   * They are ordered by PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME and SPECIFIC_NAME. Within
   * this, the return value, if any, is first. Next are the parameter descriptions in call order.
   * The column descriptions follow in column number order.
   *
   * <p>Each row in the <code>ResultSet</code> is a parameter description or column description with
   * the following fields:
   *
   * <OL>
   *   <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be <code>null</code>)
   *   <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be <code>null</code>)
   *   <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
   *   <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
   *       <UL>
   *         <LI>procedureColumnUnknown - nobody knows
   *         <LI>procedureColumnIn - IN parameter
   *         <LI>procedureColumnInOut - INOUT parameter
   *         <LI>procedureColumnOut - OUT parameter
   *         <LI>procedureColumnReturn - procedure return value
   *         <LI>procedureColumnResult - result column in <code>ResultSet</code>
   *       </UL>
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   *   <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the type name is fully
   *       qualified
   *   <LI><B>PRECISION</B> int {@code =>} precision
   *   <LI><B>LENGTH</B> int {@code =>} length in bytes of data
   *   <LI><B>SCALE</B> short {@code =>} scale - null is returned for data types where SCALE is not
   *       applicable.
   *   <LI><B>RADIX</B> short {@code =>} radix
   *   <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
   *       <UL>
   *         <LI>procedureNoNulls - does not allow NULL values
   *         <LI>procedureNullable - allows NULL values
   *         <LI>procedureNullableUnknown - nullability unknown
   *       </UL>
   *   <LI><B>REMARKS</B> String {@code =>} comment describing parameter/column
   *   <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be
   *       interpreted as a string when the value is enclosed in single quotes (may be <code>null
   * </code>)
   *       <UL>
   *         <LI>The string NULL (not enclosed in quotes) - if NULL was specified as the default
   *             value
   *         <LI>TRUNCATE (not enclosed in quotes) - if the specified default value cannot be
   *             represented without truncation
   *         <LI>NULL - if a default value was not specified
   *       </UL>
   *   <LI><B>SQL_DATA_TYPE</B> int {@code =>} reserved for future use
   *   <LI><B>SQL_DATETIME_SUB</B> int {@code =>} reserved for future use
   *   <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} the maximum length of binary and character based
   *       columns. For any other datatype the returned value is a NULL
   *   <LI><B>ORDINAL_POSITION</B> int {@code =>} the ordinal position, starting from 1, for the
   *       input and output parameters for a procedure. A value of 0 is returned if this row
   *       describes the procedure's return value. For result set columns, it is the ordinal
   *       position of the column in the result set starting from 1. If there are multiple result
   *       sets, the column ordinal positions are implementation defined.
   *   <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
   *       a column.
   *       <UL>
   *         <LI>YES --- if the column can include NULLs
   *         <LI>NO --- if the column cannot include NULLs
   *         <LI>empty string --- if the nullability for the column is unknown
   *       </UL>
   *   <LI><B>SPECIFIC_NAME</B> String {@code =>} the name which uniquely identifies this procedure
   *       within its schema.
   * </OL>
   *
   * <p><B>Note:</B> Some databases may not return the column descriptions for a procedure.
   *
   * <p>The PRECISION column represents the specified column size for the given column. For numeric
   * data, this is the maximum precision. For character data, this is the length in characters. For
   * datetime datatypes, this is the length in characters of the String representation (assuming the
   * maximum allowed precision of the fractional seconds component). For binary data, this is the
   * length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data
   * types where the column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param procedureNamePattern a procedure name pattern; must match the procedure name as it is
   *     stored in the database
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in
   *     the database
   * @return <code>ResultSet</code> - each row describes a stored procedure parameter or column
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    String fullTypeColumnName = "DTD_IDENTIFIER";

    /*
     *  Get info from information_schema.parameters
     */
    String sql =
        "SELECT SPECIFIC_SCHEMA PROCEDURE_CAT, NULL PROCEDURE_SCHEM, SPECIFIC_NAME PROCEDURE_NAME,"
            + " PARAMETER_NAME COLUMN_NAME, "
            + " CASE PARAMETER_MODE "
            + "  WHEN 'IN' THEN "
            + procedureColumnIn
            + "  WHEN 'OUT' THEN "
            + procedureColumnOut
            + "  WHEN 'INOUT' THEN "
            + procedureColumnInOut
            + "  ELSE IF(PARAMETER_MODE IS NULL,"
            + procedureColumnReturn
            + ","
            + procedureColumnUnknown
            + ")"
            + " END COLUMN_TYPE,"
            + dataTypeClause("DTD_IDENTIFIER", "")
            + " DATA_TYPE,"
            + "DATA_TYPE TYPE_NAME,"
            + " CASE DATA_TYPE"
            + DateTimeSizeClause(fullTypeColumnName)
            + "  ELSE "
            + "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"
            + Integer.MAX_VALUE
            + "), NUMERIC_PRECISION) "
            + " END `PRECISION`,"
            + " CASE DATA_TYPE"
            + DateTimeSizeClause(fullTypeColumnName)
            + "  ELSE "
            + "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"
            + Integer.MAX_VALUE
            + "), NUMERIC_PRECISION) "
            + " END `LENGTH`,"
            + "CASE DATA_TYPE "
            + DateTimeScaleClause(fullTypeColumnName)
            + " ELSE NUMERIC_SCALE END `SCALE`,"
            + "10 RADIX,"
            + procedureNullableUnknown
            + " NULLABLE,NULL REMARKS,NULL COLUMN_DEF,0 SQL_DATA_TYPE,0 SQL_DATETIME_SUB,"
            + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH ,ORDINAL_POSITION, '' IS_NULLABLE, SPECIFIC_NAME "
            + " FROM INFORMATION_SCHEMA.PARAMETERS "
            + " WHERE "
            + catalogCond("SPECIFIC_SCHEMA", catalog)
            + patternCond("SPECIFIC_NAME", procedureNamePattern)
            + patternCond("PARAMETER_NAME", columnNamePattern)
            + "  AND ROUTINE_TYPE='PROCEDURE'  "
            + " ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION";

    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the given catalog's system or user function parameters and return
   * type.
   *
   * <p>Only descriptions matching the schema, function and parameter name criteria are returned.
   * They are ordered by <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>, <code>FUNCTION_NAME
   * </code> and <code>SPECIFIC_ NAME</code>. Within this, the return value, if any, is first. Next
   * are the parameter descriptions in call order. The column descriptions follow in column number
   * order.
   *
   * <p>Each row in the <code>ResultSet</code> is a parameter description, column description or
   * return type description with the following fields:
   *
   * <OL>
   *   <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be <code>null</code>)
   *   <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be <code>null</code>)
   *   <LI><B>FUNCTION_NAME</B> String {@code =>} function name. This is the name used to invoke the
   *       function
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
   *   <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
   *       <UL>
   *         <LI>functionColumnUnknown - nobody knows
   *         <LI>functionColumnIn - IN parameter
   *         <LI>functionColumnInOut - INOUT parameter
   *         <LI>functionColumnOut - OUT parameter
   *         <LI>functionColumnReturn - function return value
   *         <LI>functionColumnResult - Indicates that the parameter or column is a column in the
   *             <code>ResultSet</code>
   *       </UL>
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
   *   <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the type name is fully
   *       qualified
   *   <LI><B>PRECISION</B> int {@code =>} precision
   *   <LI><B>LENGTH</B> int {@code =>} length in bytes of data
   *   <LI><B>SCALE</B> short {@code =>} scale - null is returned for data types where SCALE is not
   *       applicable.
   *   <LI><B>RADIX</B> short {@code =>} radix
   *   <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
   *       <UL>
   *         <LI>functionNoNulls - does not allow NULL values
   *         <LI>functionNullable - allows NULL values
   *         <LI>functionNullableUnknown - nullability unknown
   *       </UL>
   *   <LI><B>REMARKS</B> String {@code =>} comment describing column/parameter
   *   <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} the maximum length of binary and character based
   *       parameters or columns. For any other datatype the returned value is a NULL
   *   <LI><B>ORDINAL_POSITION</B> int {@code =>} the ordinal position, starting from 1, for the
   *       input and output parameters. A value of 0 is returned if this row describes the
   *       function's return value. For result set columns, it is the ordinal position of the column
   *       in the result set starting from 1.
   *   <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
   *       a parameter or column.
   *       <UL>
   *         <LI>YES --- if the parameter or column can include NULLs
   *         <LI>NO --- if the parameter or column cannot include NULLs
   *         <LI>empty string --- if the nullability for the parameter or column is unknown
   *       </UL>
   *   <LI><B>SPECIFIC_NAME</B> String {@code =>} the name which uniquely identifies this function
   *       within its schema. This is a user specified, or DBMS generated, name that may be
   *       different then the <code>FUNCTION_NAME</code> for example with overload functions
   * </OL>
   *
   * <p>The PRECISION column represents the specified column size for the given parameter or column.
   * For numeric data, this is the maximum precision. For character data, this is the length in
   * characters. For datetime datatypes, this is the length in characters of the String
   * representation (assuming the maximum allowed precision of the fractional seconds component).
   * For binary data, this is the length in bytes. For the ROWID datatype, this is the length in
   * bytes. Null is returned for data types where the column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param functionNamePattern a procedure name pattern; must match the function name as it is
   *     stored in the database
   * @param columnNamePattern a parameter name pattern; must match the parameter or column name as
   *     it is stored in the database
   * @return <code>ResultSet</code> - each row describes a user function parameter, column or return
   *     type
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.6
   */
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {

    String sql =
        parameterClause("ROUTINE_SCHEMA", "NULL", String.valueOf(functionReturn), "0")
            + " FROM INFORMATION_SCHEMA.ROUTINES "
            + " WHERE "
            + catalogCond("ROUTINE_SCHEMA", catalog)
            + patternCond("SPECIFIC_NAME", functionNamePattern)
            + patternCond("ROUTINE_NAME", columnNamePattern)
            + " AND ROUTINE_TYPE='FUNCTION'"
            + " ORDER BY FUNCTION_CAT, SPECIFIC_NAME"
            + " UNION "
            + parameterClause(
                "SPECIFIC_SCHEMA", "PARAMETER_NAME", returnTypeClause(), "ORDINAL_POSITION")
            + " FROM INFORMATION_SCHEMA.PARAMETERS "
            + " WHERE "
            + catalogCond("SPECIFIC_SCHEMA", catalog)
            + patternCond("SPECIFIC_NAME", functionNamePattern)
            + patternCond("PARAMETER_NAME", columnNamePattern)
            + " AND ROUTINE_TYPE='FUNCTION'"
            + " ORDER BY FUNCTION_CAT, SPECIFIC_NAME, ORDINAL_POSITION";
    return executeQuery(sql);
  }

  public ResultSet getSchemas() throws SQLException {
    return executeQuery("SELECT '' TABLE_SCHEM, '' TABLE_catalog  FROM DUAL WHERE 1=0");
  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return executeQuery("SELECT  ' ' table_schem, ' ' table_catalog FROM DUAL WHERE 1=0");
  }

  public ResultSet getCatalogs() throws SQLException {
    return executeQuery("SELECT SCHEMA_NAME TABLE_CAT FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY 1");
  }

  public ResultSet getTableTypes() throws SQLException {
    return executeQuery(
        "SELECT 'TABLE' TABLE_TYPE UNION SELECT 'SYSTEM VIEW' TABLE_TYPE UNION SELECT 'VIEW' TABLE_TYPE");
  }

  /**
   * Retrieves a description of the access rights for a table's columns.
   *
   * <p>Only privileges matching the column name criteria are returned. They are ordered by
   * COLUMN_NAME and PRIVILEGE.
   *
   * <p>Each privilege description has the following columns:
   *
   * <OL>
   *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
   *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <LI><B>TABLE_NAME</B> String {@code =>} table name
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
   *   <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be <code>null</code>)
   *   <LI><B>GRANTEE</B> String {@code =>} grantee of access
   *   <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT, INSERT, UPDATE, REFRENCES,
   *       ...)
   *   <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted to grant to others;
   *       "NO" if not; <code>null</code> if unknown
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in the database
   * @param columnNamePattern a column name pattern; must match the column name as it is stored in
   *     the database
   * @return <code>ResultSet</code> - each row is a column privilege description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {

    if (table == null) {
      throw new SQLException("'table' parameter must not be null");
    }
    String sql =
        "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME,"
            + " COLUMN_NAME, NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE, IS_GRANTABLE FROM "
            + " INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE "
            + catalogCond("TABLE_SCHEMA", catalog)
            + " AND "
            + " TABLE_NAME = "
            + escapeQuote(table)
            + patternCond("COLUMN_NAME", columnNamePattern)
            + " ORDER BY COLUMN_NAME, PRIVILEGE_TYPE";

    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the access rights for each table available in a catalog. Note that a
   * table privilege applies to one or more columns in the table. It would be wrong to assume that
   * this privilege applies to all columns (this may be true for some systems but is not true for
   * all.)
   *
   * <p>Only privileges matching the schema and table name criteria are returned. They are ordered
   * by <code>TABLE_CAT</code>, <code>TABLE_SCHEM</code>, <code>TABLE_NAME</code>, and <code>
   * PRIVILEGE</code>.
   *
   * <p>Each privilege description has the following columns:
   *
   * <OL>
   *   <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
   *   <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <LI><B>TABLE_NAME</B> String {@code =>} table name
   *   <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be <code>null</code>)
   *   <LI><B>GRANTEE</B> String {@code =>} grantee of access
   *   <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT, INSERT, UPDATE, REFRENCES,
   *       ...)
   *   <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted to grant to others;
   *       "NO" if not; <code>null</code> if unknown
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
   *     database
   * @return <code>ResultSet</code> - each row is a table privilege description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   */
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    String sql =
        "SELECT TABLE_SCHEMA TABLE_CAT,NULL  TABLE_SCHEM, TABLE_NAME, NULL GRANTOR,"
            + "GRANTEE, PRIVILEGE_TYPE  PRIVILEGE, IS_GRANTABLE  FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
            + " WHERE "
            + catalogCond("TABLE_SCHEMA", catalog)
            + patternCond("TABLE_NAME", tableNamePattern)
            + "ORDER BY TABLE_SCHEMA, TABLE_NAME,  PRIVILEGE_TYPE ";

    return executeQuery(sql);
  }

  /**
   * Retrieves a description of a table's columns that are automatically updated when any value in a
   * row is updated. They are unordered.
   *
   * <p>Each column description has the following columns:
   *
   * <OL>
   *   <LI><B>SCOPE</B> short {@code =>} is not used
   *   <LI><B>COLUMN_NAME</B> String {@code =>} column name
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from <code>java.sql.Types</code>
   *   <LI><B>TYPE_NAME</B> String {@code =>} Data source-dependent type name
   *   <LI><B>COLUMN_SIZE</B> int {@code =>} precision
   *   <LI><B>BUFFER_LENGTH</B> int {@code =>} length of column value in bytes
   *   <LI><B>DECIMAL_DIGITS</B> short {@code =>} scale - Null is returned for data types where
   *       DECIMAL_DIGITS is not applicable.
   *   <LI><B>PSEUDO_COLUMN</B> short {@code =>} whether this is pseudo column like an Oracle ROWID
   *       <UL>
   *         <LI>versionColumnUnknown - may or may not be pseudo column
   *         <LI>versionColumnNotPseudo - is NOT a pseudo column
   *         <LI>versionColumnPseudo - is a pseudo column
   *       </UL>
   * </OL>
   *
   * <p>The COLUMN_SIZE column represents the specified column size for the given column. For
   * numeric data, this is the maximum precision. For character data, this is the length in
   * characters. For datetime datatypes, this is the length in characters of the String
   * representation (assuming the maximum allowed precision of the fractional seconds component).
   * For binary data, this is the length in bytes. For the ROWID datatype, this is the length in
   * bytes. Null is returned for data types where the column size is not applicable.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog;<code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in the database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in the database
   * @return a <code>ResultSet</code> object in which each row is a column description
   * @throws SQLException if a database access error occurs
   */
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    String sql =
        "SELECT 0 SCOPE, ' ' COLUMN_NAME, 0 DATA_TYPE,"
            + " ' ' TYPE_NAME, 0 COLUMN_SIZE, 0 BUFFER_LENGTH,"
            + " 0 DECIMAL_DIGITS, 0 PSEUDO_COLUMN "
            + " FROM DUAL WHERE 1 = 0";
    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the foreign key columns in the given foreign key table that
   * reference the primary key or the columns representing a unique constraint of the parent table
   * (could be the same or a different table). The number of columns returned from the parent table
   * must match the number of columns that make up the foreign key. They are ordered by FKTABLE_CAT,
   * FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
   *
   * <p>Each foreign key column description has the following columns:
   *
   * <OL>
   *   <LI><B>PKTABLE_CAT</B> String {@code =>} parent key table catalog (may be <code>null</code>)
   *   <LI><B>PKTABLE_SCHEM</B> String {@code =>} parent key table schema (may be <code>null</code>)
   *   <LI><B>PKTABLE_NAME</B> String {@code =>} parent key table name
   *   <LI><B>PKCOLUMN_NAME</B> String {@code =>} parent key column name
   *   <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
   *       being exported (may be <code>null</code>)
   *   <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>
   *       ) being exported (may be <code>null</code>)
   *   <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name being exported
   *   <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name being exported
   *   <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value of 1
   *       represents the first column of the foreign key, a value of 2 would represent the second
   *       column within the foreign key).
   *   <LI><B>UPDATE_RULE</B> short {@code =>} What happens to foreign key when parent key is
   *       updated:
   *       <UL>
   *         <LI>importedNoAction - do not allow update of parent key if it has been imported
   *         <LI>importedKeyCascade - change imported key to agree with parent key update
   *         <LI>importedKeySetNull - change imported key to <code>NULL</code> if its parent key has
   *             been updated
   *         <LI>importedKeySetDefault - change imported key to default values if its parent key has
   *             been updated
   *         <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
   *       </UL>
   *   <LI><B>DELETE_RULE</B> short {@code =>} What happens to the foreign key when parent key is
   *       deleted.
   *       <UL>
   *         <LI>importedKeyNoAction - do not allow delete of parent key if it has been imported
   *         <LI>importedKeyCascade - delete rows that import a deleted key
   *         <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
   *             has been deleted
   *         <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
   *         <LI>importedKeySetDefault - change imported key to default if its parent key has been
   *             deleted
   *       </UL>
   *   <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
   *   <LI><B>PK_NAME</B> String {@code =>} parent key name (may be <code>null</code>)
   *   <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key constraints be
   *       deferred until commit
   *       <UL>
   *         <LI>importedKeyInitiallyDeferred - see SQL92 for definition
   *         <LI>importedKeyInitiallyImmediate - see SQL92 for definition
   *         <LI>importedKeyNotDeferrable - see SQL92 for definition
   *       </UL>
   * </OL>
   *
   * @param parentCatalog a catalog name; must match the catalog name as it is stored in the
   *     database; "" retrieves those without a catalog; <code>null</code> means drop catalog name
   *     from the selection criteria
   * @param parentSchema a schema name; must match the schema name as it is stored in the database;
   *     "" retrieves those without a schema; <code>null</code> means drop schema name from the
   *     selection criteria
   * @param parentTable the name of the table that exports the key; pattern, or null (means any
   *     table) value
   * @param foreignCatalog a catalog name; must match the catalog name as it is stored in the
   *     database; "" retrieves those without a catalog; <code>null</code> means drop catalog name
   *     from the selection criteria
   * @param foreignSchema a schema name; must match the schema name as it is stored in the database;
   *     "" retrieves those without a schema; <code>null</code> means drop schema name from the
   *     selection criteria
   * @param foreignTable the name of the table that imports the key; pattern, or null (means any
   *     table) value is stored in the database
   * @return <code>ResultSet</code> - each row is a foreign key column description
   * @throws SQLException if a database access error occurs
   * @see #getImportedKeys
   */
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "SingleStore does not support foreign keys and referential integrity");
  }

  /**
   * Retrieves a description of all the data types supported by this database. They are ordered by
   * DATA_TYPE and then by how closely the data type maps to the corresponding JDBC SQL type.
   *
   * <p>If the database supports SQL distinct types, then getTypeInfo() will return a single row
   * with a TYPE_NAME of DISTINCT and a DATA_TYPE of Types.DISTINCT. If the database supports SQL
   * structured types, then getTypeInfo() will return a single row with a TYPE_NAME of STRUCT and a
   * DATA_TYPE of Types.STRUCT.
   *
   * <p>If SQL distinct or structured types are supported, then information on the individual types
   * may be obtained from the getUDTs() method.
   *
   * <p>Each type description has the following columns:
   *
   * <OL>
   *   <LI><B>TYPE_NAME</B> String {@code =>} Type name
   *   <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
   *   <LI><B>PRECISION</B> int {@code =>} maximum precision
   *   <LI><B>LITERAL_PREFIX</B> String {@code =>} prefix used to quote a literal (may be <code>null
   * </code>)
   *   <LI><B>LITERAL_SUFFIX</B> String {@code =>} suffix used to quote a literal (may be <code>null
   * </code>)
   *   <LI><B>CREATE_PARAMS</B> String {@code =>} parameters used in creating the type (may be
   *       <code>null</code>)
   *   <LI><B>NULLABLE</B> short {@code =>} can you use NULL for this type.
   *       <UL>
   *         <LI>typeNoNulls - does not allow NULL values
   *         <LI>typeNullable - allows NULL values
   *         <LI>typeNullableUnknown - nullability unknown
   *       </UL>
   *   <LI><B>CASE_SENSITIVE</B> boolean{@code =>} is it case sensitive.
   *   <LI><B>SEARCHABLE</B> short {@code =>} can you use "WHERE" based on this type:
   *       <UL>
   *         <LI>typePredNone - No support
   *         <LI>typePredChar - Only supported with WHERE .. LIKE
   *         <LI>typePredBasic - Supported except for WHERE .. LIKE
   *         <LI>typeSearchable - Supported for all WHERE ..
   *       </UL>
   *   <LI><B>UNSIGNED_ATTRIBUTE</B> boolean {@code =>} is it unsigned.
   *   <LI><B>FIXED_PREC_SCALE</B> boolean {@code =>} can it be a money value.
   *   <LI><B>AUTO_INCREMENT</B> boolean {@code =>} can it be used for an auto-increment value.
   *   <LI><B>LOCAL_TYPE_NAME</B> String {@code =>} localized version of type name (may be <code>
   * null</code>)
   *   <LI><B>MINIMUM_SCALE</B> short {@code =>} minimum scale supported
   *   <LI><B>MAXIMUM_SCALE</B> short {@code =>} maximum scale supported
   *   <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
   *   <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
   *   <LI><B>NUM_PREC_RADIX</B> int {@code =>} usually 2 or 10
   * </OL>
   *
   * <p>The PRECISION column represents the maximum column size that the server supports for the
   * given datatype. For numeric data, this is the maximum precision. For character data, this is
   * the length in characters. For datetime datatypes, this is the length in characters of the
   * String representation (assuming the maximum allowed precision of the fractional seconds
   * component). For binary data, this is the length in bytes. For the ROWID datatype, this is the
   * length in bytes. Null is returned for data types where the column size is not applicable.
   *
   * @return a <code>ResultSet</code> object in which each row is an SQL type description
   */
  public ResultSet getTypeInfo() {
    String[] columnNames = {
      "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
      "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
      "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE",
      "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
    };
    DataType[] dataTypes = {
      DataType.VARCHAR,
      DataType.INT,
      DataType.INT,
      DataType.VARCHAR,
      DataType.VARCHAR,
      DataType.VARCHAR,
      DataType.INT,
      DataType.BIT,
      DataType.SMALLINT,
      DataType.BIT,
      DataType.BIT,
      DataType.BIT,
      DataType.VARCHAR,
      DataType.SMALLINT,
      DataType.SMALLINT,
      DataType.INT,
      DataType.INT,
      DataType.INT
    };

    String[][] data = {
      {"BIT", "-7", "1", "", "", "", "1", "1", "3", "0", "0", "0", "BIT", "0", "0", "0", "0", "10"},
      {
        "BOOL", "-7", "1", "", "", "", "1", "1", "3", "0", "0", "0", "BOOL", "0", "0", "0", "0",
        "10"
      },
      {
        "TINYINT",
        "-6",
        "3",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "TINYINT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "TINYINT UNSIGNED",
        "-6",
        "3",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "TINYINT UNSIGNED",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "BIGINT",
        "-5",
        "19",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "BIGINT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "BIGINT UNSIGNED",
        "-5",
        "20",
        "",
        "",
        "[(M)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "BIGINT UNSIGNED",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "LONG VARBINARY",
        "-4",
        "16777215",
        "'",
        "'",
        "",
        "1",
        "1",
        "3",
        "0",
        "0",
        "0",
        "LONG VARBINARY",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "MEDIUMBLOB",
        "-4",
        "16777215",
        "'",
        "'",
        "",
        "1",
        "1",
        "3",
        "0",
        "0",
        "0",
        "MEDIUMBLOB",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "LONGBLOB",
        "-4",
        "2147483647",
        "'",
        "'",
        "",
        "1",
        "1",
        "3",
        "0",
        "0",
        "0",
        "LONGBLOB",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "BLOB", "-4", "65535", "'", "'", "", "1", "1", "3", "0", "0", "0", "BLOB", "0", "0", "0",
        "0", "10"
      },
      {
        "TINYBLOB",
        "-4",
        "255",
        "'",
        "'",
        "",
        "1",
        "1",
        "3",
        "0",
        "0",
        "0",
        "TINYBLOB",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "VARBINARY",
        "-3",
        "255",
        "'",
        "'",
        "(M)",
        "1",
        "1",
        "3",
        "0",
        "0",
        "0",
        "VARBINARY",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "BINARY", "-2", "255", "'", "'", "(M)", "1", "1", "3", "0", "0", "0", "BINARY", "0", "0",
        "0", "0", "10"
      },
      {
        "LONG VARCHAR",
        "-1",
        "16777215",
        "'",
        "'",
        "",
        "1",
        "0",
        "3",
        "0",
        "0",
        "0",
        "LONG VARCHAR",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "MEDIUMTEXT",
        "-1",
        "16777215",
        "'",
        "'",
        "",
        "1",
        "0",
        "3",
        "0",
        "0",
        "0",
        "MEDIUMTEXT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "LONGTEXT",
        "-1",
        "2147483647",
        "'",
        "'",
        "",
        "1",
        "0",
        "3",
        "0",
        "0",
        "0",
        "LONGTEXT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "TEXT", "-1", "65535", "'", "'", "", "1", "0", "3", "0", "0", "0", "TEXT", "0", "0", "0",
        "0", "10"
      },
      {
        "TINYTEXT",
        "-1",
        "255",
        "'",
        "'",
        "",
        "1",
        "0",
        "3",
        "0",
        "0",
        "0",
        "TINYTEXT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "CHAR", "1", "255", "'", "'", "(M)", "1", "0", "3", "0", "0", "0", "CHAR", "0", "0", "0",
        "0", "10"
      },
      {
        "NUMERIC",
        "2",
        "65",
        "",
        "",
        "[(M,D])] [ZEROFILL]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "1",
        "NUMERIC",
        "-308",
        "308",
        "0",
        "0",
        "10"
      },
      {
        "DECIMAL",
        "3",
        "65",
        "",
        "",
        "[(M,D])] [ZEROFILL]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "1",
        "DECIMAL",
        "-308",
        "308",
        "0",
        "0",
        "10"
      },
      {
        "INTEGER",
        "4",
        "10",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "INTEGER",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "INTEGER UNSIGNED",
        "4",
        "10",
        "",
        "",
        "[(M)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "INTEGER UNSIGNED",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "INT",
        "4",
        "10",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "INT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "INT UNSIGNED",
        "4",
        "10",
        "",
        "",
        "[(M)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "INT UNSIGNED",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "MEDIUMINT",
        "4",
        "7",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "MEDIUMINT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "MEDIUMINT UNSIGNED",
        "4",
        "8",
        "",
        "",
        "[(M)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "MEDIUMINT UNSIGNED",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "SMALLINT",
        "5",
        "5",
        "",
        "",
        "[(M)] [UNSIGNED] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "SMALLINT",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "SMALLINT UNSIGNED",
        "5",
        "5",
        "",
        "",
        "[(M)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "1",
        "0",
        "1",
        "SMALLINT UNSIGNED",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "FLOAT",
        "7",
        "10",
        "",
        "",
        "[(M|D)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "1",
        "FLOAT",
        "-38",
        "38",
        "0",
        "0",
        "10"
      },
      {
        "DOUBLE",
        "8",
        "17",
        "",
        "",
        "[(M|D)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "1",
        "DOUBLE",
        "-308",
        "308",
        "0",
        "0",
        "10"
      },
      {
        "DOUBLE PRECISION",
        "8",
        "17",
        "",
        "",
        "[(M,D)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "1",
        "DOUBLE PRECISION",
        "-308",
        "308",
        "0",
        "0",
        "10"
      },
      {
        "REAL",
        "8",
        "17",
        "",
        "",
        "[(M,D)] [ZEROFILL]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "1",
        "REAL",
        "-308",
        "308",
        "0",
        "0",
        "10"
      },
      {
        "VARCHAR", "12", "255", "'", "'", "(M)", "1", "0", "3", "0", "0", "0", "VARCHAR", "0", "0",
        "0", "0", "10"
      },
      {
        "ENUM", "12", "65535", "'", "'", "", "1", "0", "3", "0", "0", "0", "ENUM", "0", "0", "0",
        "0", "10"
      },
      {
        "SET", "12", "64", "'", "'", "", "1", "0", "3", "0", "0", "0", "SET", "0", "0", "0", "0",
        "10"
      },
      {
        "DATE", "91", "10", "'", "'", "", "1", "0", "3", "0", "0", "0", "DATE", "0", "0", "0", "0",
        "10"
      },
      {
        "TIME", "92", "18", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0", "TIME", "0", "0", "0",
        "0", "10"
      },
      {
        "DATETIME",
        "93",
        "27",
        "'",
        "'",
        "[(M)]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "0",
        "DATETIME",
        "0",
        "0",
        "0",
        "0",
        "10"
      },
      {
        "TIMESTAMP",
        "93",
        "27",
        "'",
        "'",
        "[(M)]",
        "1",
        "0",
        "3",
        "0",
        "0",
        "0",
        "TIMESTAMP",
        "0",
        "0",
        "0",
        "0",
        "10"
      }
    };

    return CompleteResult.createResultSet(columnNames, dataTypes, data, connection.getContext(), 0);
  }

  /**
   * Retrieves a description of the given table's indices and statistics. They are ordered by
   * NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
   *
   * <p>Each index column description has the following columns:
   *
   * <ol>
   *   <li><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
   *   <li><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
   *   <li><B>TABLE_NAME</B> String {@code =>} table name
   *   <li><B>NON_UNIQUE</B> boolean {@code =>} Can index values be non-unique. false when TYPE is
   *       tableIndexStatistic
   *   <li><B>INDEX_QUALIFIER</B> String {@code =>} index catalog (may be <code>null</code>); <code>
   *       null</code> when TYPE is tableIndexStatistic
   *   <li><B>INDEX_NAME</B> String {@code =>} index name; <code>null</code> when TYPE is
   *       tableIndexStatistic
   *   <li><B>TYPE</B> short {@code =>} index type:
   *       <ul>
   *         <li>tableIndexStatistic - this identifies table statistics that are returned in
   *             conjuction with a table's index descriptions
   *         <li>tableIndexClustered - this is a clustered index
   *         <li>tableIndexHashed - this is a hashed index
   *         <li>tableIndexOther - this is some other style of index
   *       </ul>
   *   <li><B>ORDINAL_POSITION</B> short {@code =>} column sequence number within index; zero when
   *       TYPE is tableIndexStatistic
   *   <li><B>COLUMN_NAME</B> String {@code =>} column name; <code>null</code> when TYPE is
   *       tableIndexStatistic
   *   <li><B>ASC_OR_DESC</B> String {@code =>} column sort sequence, "A" {@code =>} ascending, "D"
   *       {@code =>} descending, may be <code>null</code> if sort sequence is not supported; <code>
   *       null</code> when TYPE is tableIndexStatistic
   *   <li><B>CARDINALITY</B> long {@code =>} When TYPE is tableIndexStatistic, then this is the
   *       number of rows in the table; otherwise, it is the number of unique values in the index.
   *   <li><B>PAGES</B> long {@code =>} When TYPE is tableIndexStatisic then this is the number of
   *       pages used for the table, otherwise it is the number of pages used for the current index.
   *   <li><B>FILTER_CONDITION</B> String {@code =>} Filter condition, if any. (may be <code>null
   *       </code>)
   * </ol>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in this database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schema a schema name; must match the schema name as it is stored in this database; ""
   *     retrieves those without a schema; <code>null</code> means that the schema name should not
   *     be used to narrow the search
   * @param table a table name; must match the table name as it is stored in this database
   * @param unique when true, return only indices for unique values; when false, return indices
   *     regardless of whether unique or not
   * @param approximate when true, result is allowed to reflect approximate or out of data values;
   *     when false, results are requested to be accurate
   * @return <code>ResultSet</code> - each row is an index column description
   * @throws SQLException if a database access error occurs
   */
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    if (table == null) {
      throw new SQLException("'table' parameter must not be null");
    }
    String sql =
        "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, "
            + " TABLE_SCHEMA INDEX_QUALIFIER, INDEX_NAME, "
            + tableIndexOther
            + " TYPE,"
            + " SEQ_IN_INDEX ORDINAL_POSITION, COLUMN_NAME, COLLATION ASC_OR_DESC,"
            + " CARDINALITY, NULL PAGES, NULL FILTER_CONDITION"
            + " FROM INFORMATION_SCHEMA.STATISTICS"
            + " WHERE TABLE_NAME = "
            + escapeQuote(table)
            + " AND "
            + catalogCond("TABLE_SCHEMA", catalog)
            + ((unique) ? " AND NON_UNIQUE = 0" : "")
            + " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";

    return executeQuery(sql);
  }

  /**
   * Retrieves whether this database supports the given result set type. ResultSet.TYPE_FORWARD_ONLY
   * and ResultSet.TYPE_SCROLL_INSENSITIVE are supported.
   *
   * @param type one of the following <code>ResultSet</code> constants:
   *     <ul>
   *       <li><code>ResultSet.TYPE_FORWARD_ONLY</code>
   *       <li><code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>
   *       <li><code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   *     </ul>
   *
   * @return true if supported
   */
  public boolean supportsResultSetType(int type) {
    return (type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_FORWARD_ONLY);
  }

  /**
   * Retrieves whether this database supports the given concurrency type in combination with the
   * given result set type. All are supported, but combination that use
   * ResultSet.TYPE_SCROLL_INSENSITIVE.
   *
   * @param type one of the following <code>ResultSet</code> constants:
   *     <ul>
   *       <li><code>ResultSet.TYPE_FORWARD_ONLY</code>
   *       <li><code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>
   *       <li><code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   *     </ul>
   *
   * @param concurrency one of the following <code>ResultSet</code> constants:
   *     <ul>
   *       <li><code>ResultSet.CONCUR_READ_ONLY</code>
   *       <li><code>ResultSet.CONCUR_UPDATABLE</code>
   *     </ul>
   *
   * @return true if supported
   */
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    // Support all concurrency (ResultSet.CONCUR_READ_ONLY and ResultSet.CONCUR_UPDATABLE)
    // so just return scroll type
    return type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_FORWARD_ONLY;
  }

  public boolean ownUpdatesAreVisible(int type) {
    return supportsResultSetType(type);
  }

  public boolean ownDeletesAreVisible(int type) {
    return supportsResultSetType(type);
  }

  public boolean ownInsertsAreVisible(int type) {
    return supportsResultSetType(type);
  }

  public boolean othersUpdatesAreVisible(int type) {
    return false;
  }

  public boolean othersDeletesAreVisible(int type) {
    return false;
  }

  public boolean othersInsertsAreVisible(int type) {
    return false;
  }

  public boolean updatesAreDetected(int type) {
    return false;
  }

  public boolean deletesAreDetected(int type) {
    return false;
  }

  public boolean insertsAreDetected(int type) {
    return false;
  }

  public boolean supportsBatchUpdates() {
    return true;
  }

  /**
   * Retrieves a description of the user-defined types (UDTs) defined in a particular schema.
   * Schema-specific UDTs may have type <code>JAVA_OBJECT</code>, <code>STRUCT</code>, or <code>
   * DISTINCT</code>.
   *
   * <p>Only types matching the catalog, schema, type name and type criteria are returned. They are
   * ordered by <code>DATA_TYPE</code>, <code>TYPE_CAT</code>, <code>TYPE_SCHEM</code> and <code>
   * TYPE_NAME</code>. The type name parameter may be a fully-qualified name. In this case, the
   * catalog and schemaPattern parameters are ignored.
   *
   * <p>Each type description has the following columns:
   *
   * <ol>
   *   <li><B>TYPE_CAT</B> String {@code =>} the type's catalog (may be <code>null</code>)
   *   <li><B>TYPE_SCHEM</B> String {@code =>} type's schema (may be <code>null</code>)
   *   <li><B>TYPE_NAME</B> String {@code =>} type name
   *   <li><B>CLASS_NAME</B> String {@code =>} Java class name
   *   <li><B>DATA_TYPE</B> int {@code =>} type value defined in java.sql.Types. One of JAVA_OBJECT,
   *       STRUCT, or DISTINCT
   *   <li><B>REMARKS</B> String {@code =>} explanatory comment on the type
   *   <li><B>BASE_TYPE</B> short {@code =>} type code of the source type of a DISTINCT type or the
   *       type that implements the user-generated reference type of the SELF_REFERENCING_COLUMN of
   *       a structured type as defined in java.sql.Types (<code>null</code> if DATA_TYPE is not
   *       DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
   * </ol>
   *
   * <p><B>Note:</B> If the driver does not support UDTs, an empty result set is returned.
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema pattern name; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param typeNamePattern a type name pattern; must match the type name as it is stored in the
   *     database; may be a fully qualified name
   * @param types a list of user-defined types (JAVA_OBJECT, STRUCT, or DISTINCT) to include; <code>
   *                        null</code> returns all types
   * @return <code>ResultSet</code> object in which each row describes a UDT
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.2
   */
  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    String sql =
        "SELECT ' ' TYPE_CAT, NULL TYPE_SCHEM, ' ' TYPE_NAME, ' ' CLASS_NAME, 0 DATA_TYPE, ' ' REMARKS, 0 BASE_TYPE"
            + " FROM DUAL WHERE 1=0";

    return executeQuery(sql);
  }

  public com.singlestore.jdbc.Connection getConnection() {
    return connection;
  }

  public boolean supportsSavepoints() {
    return false;
  }

  public boolean supportsNamedParameters() {
    return false;
  }

  public boolean supportsMultipleOpenResults() {
    return false;
  }

  public boolean supportsGetGeneratedKeys() {
    return true;
  }

  /**
   * Retrieves a description of the user-defined type (UDT) hierarchies defined in a particular
   * schema in this database. Only the immediate super type/ sub type relationship is modeled. Only
   * supertype information for UDTs matching the catalog, schema, and type name is returned. The
   * type name parameter may be a fully-qualified name. When the UDT name supplied is a
   * fully-qualified name, the catalog and schemaPattern parameters are ignored. If a UDT does not
   * have a direct super type, it is not listed here. A row of the <code>ResultSet</code> object
   * returned by this method describes the designated UDT and a direct supertype. A row has the
   * following columns:
   *
   * <OL>
   *   <li><B>TYPE_CAT</B> String {@code =>} the UDT's catalog (may be <code>null</code>)
   *   <li><B>TYPE_SCHEM</B> String {@code =>} UDT's schema (may be <code>null</code>)
   *   <li><B>TYPE_NAME</B> String {@code =>} type name of the UDT
   *   <li><B>SUPERTYPE_CAT</B> String {@code =>} the direct super type's catalog (may be <code>null
   * </code>)
   *   <li><B>SUPERTYPE_SCHEM</B> String {@code =>} the direct super type's schema (may be <code>
   * null</code>)
   *   <li><B>SUPERTYPE_NAME</B> String {@code =>} the direct super type's name
   * </OL>
   *
   * <p><B>Note:</B> If the driver does not support type hierarchies, an empty result set is
   * returned.
   *
   * @param catalog a catalog name; "" retrieves those without a catalog; <code>null</code> means
   *     drop catalog name from the selection criteria
   * @param schemaPattern a schema name pattern; "" retrieves those without a schema
   * @param typeNamePattern a UDT name pattern; may be a fully-qualified name
   * @return a <code>ResultSet</code> object in which a row gives information about the designated
   *     UDT
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.4
   */
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    String sql =
        "SELECT  ' ' TYPE_CAT, NULL TYPE_SCHEM, ' ' TYPE_NAME, ' ' SUPERTYPE_CAT, ' ' SUPERTYPE_SCHEM, ' '  SUPERTYPE_NAME"
            + " FROM DUAL WHERE 1=0";

    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the table hierarchies defined in a particular schema in this
   * database.
   *
   * <p>Only supertable information for tables matching the catalog, schema and table name are
   * returned. The table name parameter may be a fully-qualified name, in which case, the catalog
   * and schemaPattern parameters are ignored. If a table does not have a super table, it is not
   * listed here. Supertables have to be defined in the same catalog and schema as the sub tables.
   * Therefore, the type description does not need to include this information for the supertable.
   *
   * <p>Each type description has the following columns:
   *
   * <OL>
   *   <li><B>TABLE_CAT</B> String {@code =>} the type's catalog (may be <code>null</code>)
   *   <li><B>TABLE_SCHEM</B> String {@code =>} type's schema (may be <code>null</code>)
   *   <li><B>TABLE_NAME</B> String {@code =>} type name
   *   <li><B>SUPERTABLE_NAME</B> String {@code =>} the direct super type's name
   * </OL>
   *
   * <p><B>Note:</B> If the driver does not support type hierarchies, an empty result set is
   * returned.
   *
   * @param catalog a catalog name; "" retrieves those without a catalog; <code>null</code> means
   *     drop catalog name from the selection criteria
   * @param schemaPattern a schema name pattern; "" retrieves those without a schema
   * @param tableNamePattern a table name pattern; may be a fully-qualified name
   * @return a <code>ResultSet</code> object in which each row is a type description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.4
   */
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    String sql =
        "SELECT  ' ' TABLE_CAT, ' ' TABLE_SCHEM, ' ' TABLE_NAME, ' ' SUPERTABLE_NAME FROM DUAL WHERE 1=0";
    return executeQuery(sql);
  }

  /**
   * Retrieves a description of the given attribute of the given type for a user-defined type (UDT)
   * that is available in the given schema and catalog. Descriptions are returned only for
   * attributes of UDTs matching the catalog, schema, type, and attribute name criteria. They are
   * ordered by <code>TYPE_CAT</code>, <code>TYPE_SCHEM</code>, <code>TYPE_NAME</code> and <code>
   * ORDINAL_POSITION</code>. This description does not contain inherited attributes. The <code>
   * ResultSet</code> object that is returned has the following columns:
   *
   * <OL>
   *   <li><B>TYPE_CAT</B> String {@code =>} type catalog (may be <code>null</code>)
   *   <li><B>TYPE_SCHEM</B> String {@code =>} type schema (may be <code>null</code>)
   *   <li><B>TYPE_NAME</B> String {@code =>} type name
   *   <li><B>ATTR_NAME</B> String {@code =>} attribute name
   *   <li><B>DATA_TYPE</B> int {@code =>} attribute type SQL type from java.sql.Types
   *   <li><B>ATTR_TYPE_NAME</B> String {@code =>} Data source dependent type name. For a UDT, the
   *       type name is fully qualified. For a REF, the type name is fully qualified and represents
   *       the target type of the reference type.
   *   <li><B>ATTR_SIZE</B> int {@code =>} column size. For char or date types this is the maximum
   *       number of characters; for numeric or decimal types this is precision.
   *   <li><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
   *       for data types where DECIMAL_DIGITS is not applicable.
   *   <li><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
   *   <li><B>NULLABLE</B> int {@code =>} whether NULL is allowed
   *       <UL>
   *         <li>attributeNoNulls - might not allow NULL values
   *         <li>attributeNullable - definitely allows NULL values
   *         <li>attributeNullableUnknown - nullability unknown
   *       </UL>
   *   <li><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
   *   <li><B>ATTR_DEF</B> String {@code =>} default value (may be<code>null</code>)
   *   <li><B>SQL_DATA_TYPE</B> int {@code =>} unused
   *   <li><B>SQL_DATETIME_SUB</B> int {@code =>} unused
   *   <li><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in the
   *       column
   *   <li><B>ORDINAL_POSITION</B> int {@code =>} index of the attribute in the UDT (starting at 1)
   *   <li><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
   *       a attribute.
   *       <UL>
   *         <li>YES --- if the attribute can include NULLs
   *         <li>NO --- if the attribute cannot include NULLs
   *         <li>empty string --- if the nullability for the attribute is unknown
   *       </UL>
   *   <li><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope of a reference
   *       attribute (<code>null</code> if DATA_TYPE isn't REF)
   *   <li><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope of a reference
   *       attribute (<code>null</code> if DATA_TYPE isn't REF)
   *   <li><B>SCOPE_TABLE</B> String {@code =>} table name that is the scope of a reference
   *       attribute (<code>null</code> if the DATA_TYPE isn't REF)
   *   <li><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
   *       Ref type,SQL type from java.sql.Types (<code>null</code> if DATA_TYPE isn't DISTINCT or
   *       user-generated REF)
   * </OL>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param typeNamePattern a type name pattern; must match the type name as it is stored in the
   *     database
   * @param attributeNamePattern an attribute name pattern; must match the attribute name as it is
   *     declared in the database
   * @return a <code>ResultSet</code> object in which each row is an attribute description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.4
   */
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {

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

  public boolean supportsResultSetHoldability(int holdability) {
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return getSingleStoreVersion().getMajorVersion();
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return getSingleStoreVersion().getMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 2;
  }

  @Override
  public int getSQLStateType() {
    return sqlStateSQL99;
  }

  public boolean locatorsUpdateCopy() {
    return false;
  }

  public boolean supportsStatementPooling() {
    return false;
  }

  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  /**
   * Retrieves a list of the client info properties that the driver supports. The result set
   * contains the following columns
   *
   * <ol>
   *   <li>NAME String : The name of the client info property
   *   <li>MAX_LEN int : The maximum length of the value for the property
   *   <li>DEFAULT_VALUE String : The default value of the property
   *   <li>DESCRIPTION String : A description of the property. This will typically contain
   *       information as to where this property is stored in the database.
   * </ol>
   *
   * <p>The ResultSet is sorted by the NAME column
   *
   * @return A ResultSet object; each row is a supported client info property
   */
  public ResultSet getClientInfoProperties() {
    String[] columnNames = new String[] {"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"};

    DataType[] types =
        new DataType[] {DataType.VARCHAR, DataType.INT, DataType.VARCHAR, DataType.VARCHAR};
    String[][] data =
        new String[][] {
          new String[] {
            "ApplicationName",
            "16777215",
            "",
            "The name of the application currently utilizing the connection"
          },
          new String[] {
            "ClientUser",
            "16777215",
            "",
            "The name of the user that the application using the connection is performing work for. "
                + "This may not be the same as the user name that was used in establishing the connection."
          },
          new String[] {
            "ClientHostname",
            "16777215",
            "",
            "The hostname of the computer the application using the connection is running on"
          }
        };

    return CompleteResult.createResultSet(columnNames, types, data, connection.getContext(), 0);
  }

  /**
   * Retrieves a description of the system and user functions available in the given catalog. Only
   * system and user function descriptions matching the schema and function name criteria are
   * returned. They are ordered by <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>, <code>
   * FUNCTION_NAME</code> and <code>SPECIFIC_ NAME</code>.
   *
   * <p>Each function description has the the following columns:
   *
   * <OL>
   *   <li><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be <code>null</code>)
   *   <li><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be <code>null</code>)
   *   <li><B>FUNCTION_NAME</B> String {@code =>} function name. This is the name used to invoke the
   *       function
   *   <li><B>REMARKS</B> String {@code =>} explanatory comment on the function
   *   <li><B>FUNCTION_TYPE</B> short {@code =>} kind of function:
   *       <UL>
   *         <li>functionResultUnknown - Cannot determine if a return value or table will be
   *             returned
   *         <li>functionNoTable- Does not return a table
   *         <li>functionReturnsTable - Returns a table
   *       </UL>
   *   <li><B>SPECIFIC_NAME</B> String {@code =>} the name which uniquely identifies this function
   *       within its schema. This is a user specified, or DBMS generated, name that may be
   *       different then the <code>FUNCTION_NAME</code> for example with overload functions
   * </OL>
   *
   * <p>A user may not have permission to execute any of the functions that are returned by <code>
   * getFunctions</code>
   *
   * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
   *     retrieves those without a catalog; <code>null</code> means that the catalog name should not
   *     be used to narrow the search
   * @param schemaPattern a schema name pattern; must match the schema name as it is stored in the
   *     database; "" retrieves those without a schema; <code>null</code> means that the schema name
   *     should not be used to narrow the search
   * @param functionNamePattern a function name pattern; must match the function name as it is
   *     stored in the database
   * @return <code>ResultSet</code> - each row is a function description
   * @throws SQLException if a database access error occurs
   * @see #getSearchStringEscape
   * @since 1.6
   */
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    String sql =
        "SELECT ROUTINE_SCHEMA FUNCTION_CAT,NULL FUNCTION_SCHEM, ROUTINE_NAME FUNCTION_NAME,"
            + " ROUTINE_COMMENT REMARKS,"
            + "IF( DTD_IDENTIFIER LIKE 'TABLE', "
            + functionReturnsTable
            + ", "
            + functionNoTable
            + ") FUNCTION_TYPE, SPECIFIC_NAME "
            + " FROM INFORMATION_SCHEMA.ROUTINES "
            + " WHERE "
            + catalogCond("ROUTINE_SCHEMA", catalog)
            + patternCond("ROUTINE_NAME", functionNamePattern)
            + " AND ROUTINE_TYPE='FUNCTION'"
            + " UNION"
            + " SELECT AGGREGATE_SCHEMA FUNCTION_CAT,NULL FUNCTION_SCHEM, AGGREGATE_NAME FUNCTION_NAME,"
            + " NULL REMARKS, "
            + functionNoTable
            + " FUNCTION_TYPE, AGGREGATE_NAME SPECIFIC_NAME "
            + " FROM INFORMATION_SCHEMA.AGGREGATE_FUNCTIONS "
            + " WHERE "
            + catalogCond("AGGREGATE_SCHEMA", catalog)
            + patternCond("AGGREGATE_NAME", functionNamePattern);

    return executeQuery(sql);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("The receiver is not a wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  @Override
  public long getMaxLogicalLobSize() {
    return 4294967295L;
  }

  @Override
  public boolean supportsRefCursors() {
    return false;
  }

  public static class Identifier {
    public String schema;
    public String name;
  }
}
