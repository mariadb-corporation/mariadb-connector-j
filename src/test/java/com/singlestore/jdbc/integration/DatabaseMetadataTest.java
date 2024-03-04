// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.Statement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DatabaseMetadataTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS getProcTimePrecision");
    stmt.execute("DROP PROCEDURE IF EXISTS getProcTimePrecision2");
    stmt.execute("DROP PROCEDURE IF EXISTS testMetaCatalogProc");
    stmt.execute("DROP FUNCTION IF EXISTS testMetaCatalog");
    stmt.execute("DROP FUNCTION IF EXISTS UDTF");
    stmt.execute("DROP AGGREGATE IF EXISTS UDAF");
    stmt.execute("DROP FUNCTION IF EXISTS UDAFinit");
    stmt.execute("DROP FUNCTION IF EXISTS UDAFiter");
    stmt.execute("DROP FUNCTION IF EXISTS UDAFmerge");
    stmt.execute("DROP FUNCTION IF EXISTS UDAFterminate");
    stmt.execute("DROP TABLE IF EXISTS json_test");
    stmt.execute("DROP TABLE IF EXISTS pk_test");
    stmt.execute("DROP TABLE IF EXISTS pk_test_multi");
    stmt.execute("DROP TABLE IF EXISTS pk_test_rowstore");
    stmt.execute("DROP TABLE IF EXISTS pk_test_rowstore_multi");
    stmt.execute("DROP TABLE IF EXISTS datetime_test");
    stmt.execute("DROP TABLE IF EXISTS manycols");
    stmt.execute("DROP TABLE IF EXISTS ytab");
    stmt.execute("DROP TABLE IF EXISTS maxcharlength");
    stmt.execute("DROP TABLE IF EXISTS conj72");
    stmt.execute("DROP TABLE IF EXISTS getTimePrecision");
    stmt.execute("DROP TABLE IF EXISTS getPrecision");
    stmt.execute("DROP TABLE IF EXISTS versionTable");
    stmt.execute("DROP TABLE IF EXISTS tablegetcolumns");
    stmt.execute("drop table if exists getBestRowIdentifier1");
    stmt.execute("drop table if exists getBestRowIdentifier2");
    stmt.execute("drop table if exists get_index_info");
    stmt.execute("drop table if exists text_types_text");
  }

  @BeforeAll
  public static void initClass() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS getTimePrecision(" + "d date, " + "t1 datetime(0),"
        + "t2 datetime(6)," + "t3 timestamp(0) DEFAULT '2000-01-01 00:00:00',"
        + "t4 timestamp(6) DEFAULT '2000-01-01 00:00:00'," + "t5 time(0)," + "t6 time(6))");
    stmt.execute("CREATE TABLE json_test(t1 JSON)");
    stmt.execute("CREATE FUNCTION testMetaCatalog(x int) RETURNS int AS \nBEGIN\n return 1; end\n");
    stmt.execute("CREATE"
        + " TABLE IF NOT EXISTS pk_test(val varchar(20), id int not null, primary key(id))");
    stmt.execute("CREATE"
        + " TABLE IF NOT EXISTS pk_test_multi(val varchar(20), id1 int not null, id2 int not null,primary key(id1, "
        + "id2))");
    stmt.execute(createRowstore()
        + " TABLE IF NOT EXISTS pk_test_rowstore(val varchar(20), id int not null,primary key(id))");
    stmt.execute(createRowstore()
        + " TABLE IF NOT EXISTS pk_test_rowstore_multi(val varchar(20), id1 int not null, id2 int not null,primary key(id1, "
        + "id2))");
    stmt.execute(
        "CREATE FUNCTION UDTF(x int) returns table as return select * from pk_test where id = x");
    stmt.execute(
        "CREATE FUNCTION UDAFinit () RETURNS bigint AS declare s bigint ; BEGIN RETURN s; END");
    stmt.execute("CREATE FUNCTION UDAFiter(s bigint , v bigint ) " + "RETURNS bigint AS " + "BEGIN "
        + "IF (v is not null and s is null) or ( v > s ) THEN " + "return v; " + "END IF; "
        + "RETURN s; " + "END");
    stmt.execute("CREATE FUNCTION UDAFmerge(s1 bigint , s2 bigint ) " + "RETURNS bigint AS "
        + "BEGIN " + "IF s2 > s1 THEN " + "RETURN s2; " + "END IF; " + "RETURN s1; " + "END");
    stmt.execute("CREATE FUNCTION UDAFterminate(s bigint) RETURNS bigint AS BEGIN RETURN s; END");
    stmt.execute("CREATE AGGREGATE UDAF(bigint) RETURNS bigint " + "WITH STATE bigint "
        + "INITIALIZE WITH UDAFinit " + "ITERATE WITH UDAFiter " + "MERGE WITH UDAFmerge "
        + "TERMINATE WITH UDAFterminate;");
    stmt.execute("CREATE TABLE IF NOT EXISTS datetime_test(dt datetime)");
    stmt.execute("CREATE TABLE IF NOT EXISTS `manycols`(" + "  `tiny` tinyint(4) DEFAULT NULL,"
        + "  `tiny_uns` tinyint(3) unsigned DEFAULT NULL," + "  `small` smallint(6) DEFAULT NULL,"
        + "  `small_uns` smallint(5) unsigned DEFAULT NULL,"
        + "  `medium` mediumint(9) DEFAULT NULL,"
        + "  `medium_uns` mediumint(8) unsigned DEFAULT NULL," + "  `int_col` int(11) DEFAULT NULL,"
        + "  `int_col_uns` int(10) unsigned DEFAULT NULL," + "  `big` bigint(20) DEFAULT NULL,"
        + "  `big_uns` bigint(20) unsigned DEFAULT NULL,"
        + "  `decimal_col` decimal(10,5) DEFAULT NULL," + "  `fcol` float DEFAULT NULL,"
        + "  `fcol_uns` float unsigned DEFAULT NULL," + "  `dcol` double DEFAULT NULL,"
        + "  `dcol_uns` double unsigned DEFAULT NULL," + "  `date_col` date DEFAULT NULL,"
        + "  `time_col` time DEFAULT NULL,"
        + "  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        + "  `year_col` year(4) DEFAULT NULL," + "  `bit_col` bit(5) DEFAULT NULL,"
        + "  `char_col` char(5) DEFAULT NULL," + "  `varchar_col` varchar(10) DEFAULT NULL,"
        + "  `binary_col` binary(10) DEFAULT NULL,"
        + "  `varbinary_col` varbinary(10) DEFAULT NULL," + "  `tinyblob_col` tinyblob,"
        + "  `blob_col` blob," + "  `mediumblob_col` mediumblob," + "  `longblob_col` longblob,"
        + "  `tinytext_col` tinytext," + "  `text_col` text," + "  `mediumtext_col` mediumtext,"
        + "  `longtext_col` longtext)");
    stmt.execute("CREATE TABLE IF NOT EXISTS ytab(y year)");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS maxcharlength(maxcharlength char(1)) character set utf8");
    stmt.execute("CREATE TABLE IF NOT EXISTS conj72(t tinyint(1))");

    stmt.execute(
        "create table getBestRowIdentifier1 (id int not null primary key, val varchar(20))");
    stmt.execute(
        "create table getBestRowIdentifier2(id int not null auto_increment, SHARD KEY(id), UNIQUE KEY(id))");
    stmt.execute("CREATE TABLE IF NOT EXISTS get_index_info(\n"
        + "    no INT NOT NULL AUTO_INCREMENT,\n" + "    product_category INT NOT NULL,\n"
        + "    product_id INT NOT NULL,\n" + "    customer_id INT NOT NULL,\n"
        + "    PRIMARY KEY(no),\n" + "    INDEX ind_prod (product_category, product_id),\n"
        + "    INDEX ind_cust (customer_id))");
    stmt.execute("CREATE PROCEDURE getProcTimePrecision2( I date, " + "t1 DATETIME,"
        + "t3 timestamp," + "t5 time) AS BEGIN ECHO SELECT I; END");
    stmt.execute("CREATE PROCEDURE getProcTimePrecision" + "(I date, " + "t1 DATETIME(0),"
        + "t2 DATETIME(6)," + "t3 timestamp(0)," + "t4 timestamp(6)," + "t5 time ,"
        + "t6 time(6)) AS BEGIN ECHO SELECT I; END");
    stmt.execute(
        "CREATE PROCEDURE testMetaCatalogProc(x int) RETURNS int AS \nBEGIN\n return 1; end\n");
    stmt.execute("create table text_types_text (varchar100           varchar(100),\n"
        + "  varchar255           varchar(255),\n" + "  text                 text,\n"
        + "  `tinytext`           tinytext,\n" + "  `mediumtext`         mediumtext,\n"
        + "  `longtext`           longtext)");
  }

  private static void checkType(String name, int actualType, String colName, int expectedType) {
    if (name.equals(colName)) {
      assertEquals(actualType, expectedType);
    }
  }

  @Test
  public void primaryKeysTest() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getPrimaryKeys(sharedConn.getCatalog(), null, "pk_test");

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString("table_cat"));
    assertNull(rs.getString("table_schem"));
    assertEquals("pk_test", rs.getString("table_name"));
    assertEquals("id", rs.getString("column_name"));
    assertEquals("PRIMARY", rs.getString("PK_NAME"));
    assertFalse(rs.next());

    rs = meta.getPrimaryKeys(sharedConn.getCatalog(), null, "pk_test_multi");
    int counter = 0;
    while (rs.next()) {
      counter++;
      assertEquals(sharedConn.getCatalog(), rs.getString("table_cat"));
      assertNull(rs.getString("table_schem"));
      assertEquals("pk_test_multi", rs.getString("table_name"));
      assertEquals("id" + counter, rs.getString("column_name"));
      assertEquals("PRIMARY", rs.getString("PK_NAME"));
    }
    assertEquals(2, counter);

    rs = meta.getPrimaryKeys(sharedConn.getCatalog(), null, "pk_test_rowstore");

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString("table_cat"));
    assertNull(rs.getString("table_schem"));
    assertEquals("pk_test_rowstore", rs.getString("table_name"));
    assertEquals("id", rs.getString("column_name"));
    assertEquals("PRIMARY", rs.getString("PK_NAME"));
    assertFalse(rs.next());

    rs = meta.getPrimaryKeys(sharedConn.getCatalog(), null, "pk_test_rowstore_multi");
    counter = 0;
    while (rs.next()) {
      counter++;
      assertEquals(sharedConn.getCatalog(), rs.getString("table_cat"));
      assertNull(rs.getString("table_schem"));
      assertEquals("pk_test_rowstore_multi", rs.getString("table_name"));
      assertEquals("id" + counter, rs.getString("column_name"));
      assertEquals("PRIMARY", rs.getString("PK_NAME"));
    }
    assertEquals(2, counter);
  }

  @Test
  public void datetimeTest() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from datetime_test");
    assertEquals(93, rs.getMetaData().getColumnType(1));
  }

  @Test
  public void functionColumns() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    DatabaseMetaData meta = sharedConn.getMetaData();

    stmt.execute("DROP FUNCTION IF EXISTS hello");
    stmt.execute("CREATE FUNCTION hello (s CHAR(20), i int) RETURNS CHAR(50) AS BEGIN "
        + "RETURN CONCAT('Hello, ',s,'!'); " + "END");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs = meta.getFunctionColumns(null, null, "hello", null);

    assertTrue(rs.next());
    /* First row is for return value */
    assertEquals(sharedConn.getCatalog(), rs.getString("FUNCTION_CAT"));
    assertNull(rs.getString("FUNCTION_SCHEM"));
    assertNull(rs.getString("COLUMN_NAME")); /* No name, since it is return value */
    assertEquals(DatabaseMetaData.functionReturn, rs.getInt("COLUMN_TYPE"));
    assertEquals(Types.CHAR, rs.getInt("DATA_TYPE"));
    assertEquals("CHAR", rs.getString("TYPE_NAME"));

    assertTrue(rs.next());
    assertEquals("s", rs.getString("COLUMN_NAME")); /* input parameter 's' (CHAR) */
    assertEquals(DatabaseMetaData.functionColumnIn, rs.getInt("COLUMN_TYPE"));
    assertEquals(Types.CHAR, rs.getInt("DATA_TYPE"));
    assertEquals("CHAR", rs.getString("TYPE_NAME"));

    assertTrue(rs.next());
    assertEquals("i", rs.getString("COLUMN_NAME")); /* input parameter 'i' (INT) */
    assertEquals(DatabaseMetaData.functionColumnIn, rs.getInt("COLUMN_TYPE"));
    assertEquals(Types.INTEGER, rs.getInt("DATA_TYPE"));
    assertEquals("INT", rs.getString("TYPE_NAME"));
    stmt.execute("DROP FUNCTION IF EXISTS hello");
  }

  @Test
  public void functionColumnTypes() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    DatabaseMetaData meta = sharedConn.getMetaData();

    stmt.execute("DROP FUNCTION IF EXISTS funcTypeTest");
    stmt.execute("CREATE FUNCTION funcTypeTest (a1 VARCHAR(9) , "
        + "a2 VARBINARY(10), a3 LONGTEXT, a4 MEDIUMTEXT, a5 TEXT, a6 TINYTEXT, a7 VARCHAR(11) CHARACTER SET binary, "
        + "b1 TINYBLOB, b2 BLOB, b3 MEDIUMBLOB, b4 LONGBLOB) RETURNS CHAR(50) AS BEGIN "
        + "RETURN CONCAT('Hello, ',a1,'!'); " + "END");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs = meta.getFunctionColumns(null, null, "funcTypeTest", null);
    /* First row is for return value */
    rs.next();
    rs.next();
    assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.VARBINARY, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.LONGVARCHAR, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.LONGVARCHAR, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.LONGVARCHAR, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.VARCHAR, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.VARBINARY, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.VARBINARY, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.LONGVARBINARY, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.LONGVARBINARY, rs.getInt("DATA_TYPE"));
    rs.next();
    assertEquals(Types.LONGVARBINARY, rs.getInt("DATA_TYPE"));
    rs.next();
    stmt.execute("DROP FUNCTION IF EXISTS hello");
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();

    ResultSet rs = dbmd.getCatalogs();
    boolean haveMysql = false;
    boolean haveInformationSchema = false;
    while (rs.next()) {
      String cat = rs.getString(1);

      if (cat.equalsIgnoreCase("memsql")) {
        haveMysql = true;
      } else if (cat.equalsIgnoreCase("information_schema")) {
        haveInformationSchema = true;
      }
    }
    assertTrue(haveMysql);
    assertTrue(haveInformationSchema);
  }

  @Test
  public void testGetTables() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("drop table if exists prim_key");

    stmt.execute("create table prim_key (id int not null primary key, val varchar(20))");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getTables(null, null, "prim_key", null);

    assertEquals(true, rs.next());
    rs = dbmd.getTables("", null, "prim_key", null);
    assertEquals(true, rs.next());

    rs = dbmd.getTables("", null, "prim_key", new String[] {"BASE TABLE", "OTHER"});
    assertEquals(true, rs.next());

    rs = dbmd.getTables("", null, "prim_key", new String[] {"TABLE", null});
    assertEquals(true, rs.next());

    rs = dbmd.getTables(null, null, null, new String[] {"TABLE"});
    assertEquals(true, rs.next());
  }

  @Test
  public void testGetTables2() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getTables("information_schema", null, "TABLE_PRIVILEGES",
        new String[] {"SYSTEM VIEW"});
    assertEquals(true, rs.next());
    assertEquals(false, rs.next());
    rs = dbmd.getTables("information_schema", null, "TABLE_PRIVILEGES", new String[] {"VIEW"});
    assertEquals(true, rs.next());
    assertEquals(false, rs.next());
    rs = dbmd.getTables(null, null, "TABLE_PRIVILEGES", new String[] {"TABLE"});
    assertEquals(false, rs.next());
  }

  @Test
  public void testGetTables3() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("drop table if exists table_type_test");

    stmt.execute(
        "create table table_type_test (id int not null primary key, " + "val varchar(20))");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet tableSet = dbmd.getTables(null, null, "table_type_test", null);

    assertEquals(true, tableSet.next());

    String tableName = tableSet.getString("TABLE_NAME");
    assertEquals("table_type_test", tableName);

    String tableType = tableSet.getString("TABLE_TYPE");
    assertEquals("TABLE", tableType);
    // see for possible values
    // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTableTypes%28%29
  }

  @Test
  public void testGetTable4() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getTables("information_schema", null, "TABLE_PRIVILEGES",
        new String[] {"SYSTEM VIEW"});
    assertEquals(true, rs.next());
    String tableType = rs.getString("TABLE_TYPE");
    assertEquals("VIEW", tableType);
    rs = dbmd.getTables("information_schema", null, "TABLE_PRIVILEGES", new String[] {"VIEW"});
    assertEquals(true, rs.next());
    tableType = rs.getString("TABLE_TYPE");
    assertEquals("VIEW", tableType);
  }

  @Test
  public void testGetTablesNoTVF() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS test_table");

    stmt.execute("CREATE TABLE test_table (id INT NOT NULL PRIMARY KEY, " + "val VARCHAR(20))");

    stmt.execute("CREATE OR REPLACE FUNCTION test_table_func(n int) RETURNS TABLE AS "
        + "RETURN SELECT val FROM test_table WHERE id=n;");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet tableSet = dbmd.getTables(null, null, "test_table", null);
    assertTrue(tableSet.next());

    tableSet = dbmd.getTables(null, null, "test_table_func", null);
    assertFalse(tableSet.next());
  }

  @Test
  public void testGetColumns() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE IF NOT EXISTS `table'getcolumns`("
        + "a INT NOT NULL primary key auto_increment, b VARCHAR(32), c AS (CHAR_LENGTH(b)) PERSISTED INT, "
        + "d AS left(b,5) PERSISTED VARCHAR(5) CHARACTER SET 'utf8mb4', e AS a * 2 PERSISTED INT(14) UNSIGNED)");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "table'getcolumns", null);

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertEquals(null, rs.getString(2)); // TABLE_SCHEM
    assertEquals("table'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("a", rs.getString(4)); // COLUMN_NAME
    // In S2 type of auto_increment column is set to BIGINT for sharded tables
    assertEquals(Types.BIGINT, rs.getInt(5)); // DATA_TYPE
    assertEquals("BIGINT", rs.getString(6)); // "TYPE_NAME
    assertEquals(19, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(0, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS
    assertEquals(null, rs.getString(13)); // COLUMN_DEF
    assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(1, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("NO", rs.getString(18)); // IS_NULLABLE
    assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
    assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
    assertEquals(null, rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("YES", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertEquals(null, rs.getString(2)); // TABLE_SCHEM
    assertEquals("table'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("b", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPEf
    assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
    assertEquals(32, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS

    assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
    assertEquals(32 * 3, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(2, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("YES", rs.getString(18)); // IS_NULLABLE
    assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
    assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
    assertEquals(null, rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertEquals(null, rs.getString(2)); // TABLE_SCHEM
    assertEquals("table'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("c", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
    assertEquals("INT", rs.getString(6)); // TYPE_NAME
    assertEquals(10, rs.getInt(7)); // COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS

    // since 10.2.7, value that are expected as String are enclosed with single
    // quotes as javadoc
    // require
    assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF

    assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(3, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("YES", rs.getString(18)); // IS_NULLABLE
    assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
    assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
    assertEquals(null, rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertEquals(null, rs.getString(2)); // TABLE_SCHEM
    assertEquals("table'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("d", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
    assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
    assertEquals(5, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS
    assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
    assertEquals(5 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(4, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("YES", rs.getString(18)); // IS_NULLABLE
    assertEquals(null, rs.getString(19)); // SCOPE_CATALOG
    assertEquals(null, rs.getString(20)); // SCOPE_SCHEMA
    assertEquals(null, rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertEquals(null, rs.getString(2)); // TABLE_SCHEM
    assertEquals("table'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("e", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
    assertEquals("INT UNSIGNED", rs.getString(6)); // TYPE_NAME
    assertEquals(10, rs.getInt(7)); // COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS

    assertFalse(rs.next());
  }

  @Test
  public void testGetColumnstinyInt1isBit() throws SQLException {
    try (Connection con = createCon("tinyInt1isBit=false")) {
      testGetColumnstinyInt1isBit(con);
    }
    try (Connection con = createCon("tinyInt1isBit=false")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      testGetColumnstinyInt1isBit(con);
    }
  }

  @Test
  public void testGetColumnsNoTVF() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS test_table");

    stmt.execute("CREATE TABLE test_table (id INT NOT NULL PRIMARY KEY, " + "val VARCHAR(20))");

    stmt.execute("CREATE OR REPLACE FUNCTION test_table_func(n int) RETURNS TABLE AS "
        + "RETURN SELECT val FROM test_table WHERE id=n;");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "test_table", null);
    assertTrue(rs.next());
    assertTrue(rs.next());

    rs = dbmd.getColumns(null, null, "test_table_func", null);
    assertFalse(rs.next());
  }

  private void testGetColumnstinyInt1isBit(Connection con) throws SQLException {
    try {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `tinyInt1IsBitCols`(id1 tinyint(1), id2 tinyint(2))");
      DatabaseMetaData dbmd = sharedConn.getMetaData();
      ResultSet rs = dbmd.getColumns(null, null, "tinyInt1IsBitCols", null);

      assertTrue(rs.next());
      assertEquals(Types.BIT, rs.getInt(5));
      assertTrue(rs.next());
      assertEquals(Types.BIT, rs.getInt(5));

      dbmd = con.getMetaData();
      rs = dbmd.getColumns(null, null, "tinyInt1IsBitCols", null);

      assertTrue(rs.next());
      assertEquals(Types.TINYINT, rs.getInt(5));
      assertTrue(rs.next());
      assertEquals(Types.TINYINT, rs.getInt(5));

    } finally {
      con.createStatement().execute("DROP TABLE IF EXISTS `tinyInt1IsBitCols`");
    }
  }

  @Test
  public void testResultSetMetaDataTransformedBitIsBoolean() throws SQLException {
    try (Connection con = createCon("tinyInt1isBit=false")) {
      testResultSetMetaDataTransformedBitIsBoolean(con);
    }
    try (Connection con = createCon("tinyInt1isBit=true")) {
      testResultSetMetaDataTransformedBitIsBoolean(con);
    }
    try (Connection con = createCon("tinyInt1isBit=true&transformedBitIsBoolean=true")) {
      testResultSetMetaDataTransformedBitIsBoolean(con);
    }
  }

  private void testResultSetMetaDataTransformedBitIsBoolean(Connection con) throws SQLException {
    try {
      Configuration conf = ((com.singlestore.jdbc.Connection) con).getContext().getConf();
      java.sql.Statement stmt = con.createStatement();
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `tinyInt1IsBitCols`(id1 TINYINT, id2 TINYINT UNSIGNED)");
      stmt.execute("INSERT INTO `tinyInt1IsBitCols` VALUES (2,0)");
      ResultSet rs1 = con.createStatement().executeQuery("SELECT * FROM `tinyInt1IsBitCols`");
      assertTrue(rs1.next());
      if (conf.tinyInt1isBit()) {
        if (conf.transformedBitIsBoolean()) {
          assertEquals(true, rs1.getObject(1));
          assertEquals(false, rs1.getObject(2));
          ResultSetMetaData rsm = rs1.getMetaData();
          assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
          assertEquals("BOOLEAN", rsm.getColumnTypeName(1));
          assertEquals(Types.BOOLEAN, rsm.getColumnType(2));
          assertEquals("BOOLEAN", rsm.getColumnTypeName(2));
        } else {
          assertEquals((byte) 2, rs1.getObject(1));
          assertEquals((short) 0, rs1.getObject(2));
          ResultSetMetaData rsm = rs1.getMetaData();
          assertEquals(Types.BIT, rsm.getColumnType(1));
          assertEquals("BIT", rsm.getColumnTypeName(1));
          assertEquals(Types.BIT, rsm.getColumnType(2));
          assertEquals("BIT", rsm.getColumnTypeName(2));
        }
      } else {
        assertEquals((byte) 2, rs1.getObject(1));
        assertEquals((short) 0, rs1.getObject(2));
        ResultSetMetaData rsm = rs1.getMetaData();
        assertEquals(Types.TINYINT, rsm.getColumnType(1));
        assertEquals("TINYINT", rsm.getColumnTypeName(1));
        assertEquals(Types.SMALLINT, rsm.getColumnType(2));
        assertEquals("TINYINT UNSIGNED", rsm.getColumnTypeName(2));
      }
    } finally {
      con.createStatement().execute("DROP TABLE IF EXISTS `tinyInt1IsBitCols`");
    }
  }

  @Test
  public void testDataBaseMetadataTransformedBitIsBoolean() throws SQLException {
    try (Connection con = createCon("tinyInt1isBit=false")) {
      testDataBaseMetaDataTransformedBitIsBoolean(con);
    }
    try (Connection con = createCon("tinyInt1isBit=true")) {
      testDataBaseMetaDataTransformedBitIsBoolean(con);
    }
    try (Connection con = createCon("tinyInt1isBit=true&transformedBitIsBoolean=true")) {
      testDataBaseMetaDataTransformedBitIsBoolean(con);
    }
  }

  private void testDataBaseMetaDataTransformedBitIsBoolean(Connection con) throws SQLException {
    try {
      Configuration conf = ((com.singlestore.jdbc.Connection) con).getContext().getConf();
      java.sql.Statement stmt = con.createStatement();
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `tinyInt1IsBitCols`(id1 TINYINT, id2 TINYINT UNSIGNED)");
      stmt.execute("INSERT INTO `tinyInt1IsBitCols` VALUES (2,0)");
      DatabaseMetaData databaseMetaData = con.getMetaData();
      ResultSet rs2 = databaseMetaData.getColumns(conf.database(), null, "tinyInt1IsBitCols", null);
      String id1TypeName = null;
      String id2TypeName = null;
      int id1Type = Types.NULL;
      int id2Type = Types.NULL;
      while (rs2.next()) {
        String name = rs2.getString(4);
        if ("id1".equals(name)) {
          id1Type = rs2.getInt(5);
          id1TypeName = rs2.getString(6);
        } else if ("id2".equals(name)) {
          id2Type = rs2.getInt(5);
          id2TypeName = rs2.getString(6);
        } else {
          Assertions.fail("Wrong metadata column name " + name);
        }
      }
      if (conf.tinyInt1isBit()) {
        if (conf.transformedBitIsBoolean()) {
          assertEquals(Types.BOOLEAN, id1Type);
          assertEquals("BOOLEAN", id1TypeName);
          assertEquals(Types.BOOLEAN, id2Type);
          assertEquals("BOOLEAN", id2TypeName);
        } else {
          assertEquals(Types.BIT, id1Type);
          assertEquals("BIT", id1TypeName);
          assertEquals(Types.BIT, id2Type);
          assertEquals("BIT", id2TypeName);
        }
      } else {
        assertEquals(Types.TINYINT, id1Type);
        assertEquals("TINYINT", id1TypeName);
        assertEquals(Types.TINYINT, id2Type);
        assertEquals("TINYINT UNSIGNED", id2TypeName);
      }
    } finally {
      con.createStatement().execute("DROP TABLE IF EXISTS `tinyInt1IsBitCols`");
    }
  }

  private void testResultSetColumns(ResultSet rs, String spec) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    String[] tokens = spec.split(",");

    for (int i = 0; i < tokens.length; i++) {
      String[] splitTokens = tokens[i].trim().split(" ");
      String label = splitTokens[0];
      String type = splitTokens[1];

      int col = i + 1;
      assertEquals(label, rsmd.getColumnLabel(col));
      int columnType = rsmd.getColumnType(col);
      switch (type) {
        case "String":
          assertTrue(
              columnType == Types.VARCHAR || columnType == Types.CHAR || columnType == Types.NULL
                  || columnType == Types.LONGVARCHAR,
              "invalid type " + columnType + " for " + rsmd.getColumnLabel(col)
                  + ",expected String");
          break;
        case "decimal":
          assertTrue(columnType == Types.DECIMAL,
              "invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col) + " ) for "
                  + rsmd.getColumnLabel(col) + ",expected decimal");
          break;
        case "int":
        case "short":
          assertTrue(
              columnType == Types.BIGINT || columnType == Types.INTEGER
                  || columnType == Types.SMALLINT || columnType == Types.TINYINT,
              "invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col) + " ) for "
                  + rsmd.getColumnLabel(col) + ", expected numeric");

          break;
        case "boolean":
          assertTrue(columnType == Types.BOOLEAN || columnType == Types.BIT,
              "invalid type  " + columnType + "( " + rsmd.getColumnTypeName(col) + " ) for "
                  + rsmd.getColumnLabel(col) + ",expected boolean");

          break;
        case "null":
          assertTrue(columnType == Types.NULL, "invalid type  " + columnType + " for "
              + rsmd.getColumnLabel(col) + ",expected null");
          break;
        default:
          fail("invalid type '" + type + "'");
          break;
      }
    }
  }

  @Test
  public void getSchemas() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getSchemas();
    assertFalse(rs.next());

    rs = dbmd.getSchemas("*", "*");
    assertFalse(rs.next());
  }

  @Test
  public void getAttributesBasic() throws Exception {
    testResultSetColumns(sharedConn.getMetaData().getAttributes(null, null, null, null),
        "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,"
            + "ATTR_NAME String,DATA_TYPE int,ATTR_TYPE_NAME String,ATTR_SIZE int,DECIMAL_DIGITS int,"
            + "NUM_PREC_RADIX int,NULLABLE int,REMARKS String,ATTR_DEF String,SQL_DATA_TYPE int,"
            + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE String,"
            + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
            + "SCOPE_TABLE String,SOURCE_DATA_TYPE short");
  }

  @Test
  public void identifierCaseSensitivity() throws Exception {
    java.sql.Statement stmt = sharedConn.createStatement();
    try {
      if (sharedConn.getMetaData().supportsMixedCaseIdentifiers()) {
        /* Case-sensitive identifier handling, we can create both t1 and T1 */
        stmt.execute("create table aB (i int)");
        stmt.execute("create table AB (i int)");
        /* Check there is an entry for both T1 and t1 in getTables */
        ResultSet rs = sharedConn.getMetaData().getTables(null, null, "aB", null);
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs = sharedConn.getMetaData().getTables(null, null, "AB", null);
        assertTrue(rs.next());
        assertFalse(rs.next());
      }

      if (sharedConn.getMetaData().storesMixedCaseIdentifiers()) {
        /* Case-insensitive, case-preserving */
        stmt.execute("create table aB (i int)");
        try {
          stmt.execute("create table AB (i int)");
          fail("should not get there, since names are case-insensitive");
        } catch (SQLException e) {
          // normal error
        }

        /* Check that table is stored case-preserving */
        ResultSet rs = sharedConn.getMetaData().getTables(null, null, "aB%", null);
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if (tableName.length() == 2) {
            assertEquals("aB", tableName);
          }
        }

        rs = sharedConn.getMetaData().getTables(null, null, "AB", null);
        assertTrue(rs.next());
        assertFalse(rs.next());
      }

      if (sharedConn.getMetaData().storesLowerCaseIdentifiers()) {
        /* case-insensitive, identifiers converted to lowercase */
        /* Case-insensitive, case-preserving */
        stmt.execute("create table aB (i int)");
        try {
          stmt.execute("create table AB (i int)");
          fail("should not get there, since names are case-insensitive");
        } catch (SQLException e) {
          // normal error
        }

        /* Check that table is stored lowercase */
        ResultSet rs = sharedConn.getMetaData().getTables(null, null, "aB%", null);
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          if (tableName.length() == 2) {
            assertEquals("ab", tableName);
          }
        }

        rs = sharedConn.getMetaData().getTables(null, null, "AB", null);
        assertTrue(rs.next());
        assertFalse(rs.next());
      }
      assertFalse(sharedConn.getMetaData().storesUpperCaseIdentifiers());
    } finally {
      try {
        stmt.execute("DROP TABLE aB");
      } catch (SQLException sqle) {
        // ignore
      }
      try {
        stmt.execute("DROP TABLE AB");
      } catch (SQLException sqle) {
        // ignore
      }
    }
  }

  @Test
  public void getBestRowIdentifier() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    Common.assertThrowsContains(SQLException.class,
        () -> meta.getBestRowIdentifier(null, null, null, 0, true),
        "'table' parameter cannot be null in getBestRowIdentifier()");
    testResultSetColumns(meta.getBestRowIdentifier(null, null, "", 0, true),
        "SCOPE short,COLUMN_NAME String,DATA_TYPE int, TYPE_NAME String,"
            + "COLUMN_SIZE int,BUFFER_LENGTH int," + "DECIMAL_DIGITS short,PSEUDO_COLUMN short");

    ResultSet rs = meta.getBestRowIdentifier(null, null, "getBestRowIdentifier1", 0, true);
    assertTrue(rs.next());

    assertEquals(com.singlestore.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(com.singlestore.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());

    rs = meta.getBestRowIdentifier(null, null, "getBestRowIdentifier1", 0, false);
    assertTrue(rs.next());
    assertFalse(rs.next());

    // CHECK using PRI even if exist UNI
    rs = meta.getBestRowIdentifier(null, null, "getBestRowIdentifier2", 0, true);
    assertTrue(rs.next());

    assertEquals(com.singlestore.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id", rs.getString(2));
    assertEquals(Types.BIGINT, rs.getInt(3));
    assertEquals("bigint", rs.getString(4));
    assertEquals(19, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(com.singlestore.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());
  }

  @Test
  public void getClientInfoPropertiesBasic() throws Exception {
    testResultSetColumns(sharedConn.getMetaData().getClientInfoProperties(),
        "NAME String, MAX_LEN int, DEFAULT_VALUE String, DESCRIPTION String");
    ResultSet rs = sharedConn.getMetaData().getClientInfoProperties();
    assertTrue(rs.next());
    assertEquals("ApplicationName", rs.getString(1));
    assertEquals(0x00ffffff, rs.getInt(2));
    assertEquals("", rs.getString(3));
    assertEquals("The name of the application currently utilizing the connection", rs.getString(4));

    assertTrue(rs.next());
    assertEquals("ClientUser", rs.getString(1));
    assertEquals(0x00ffffff, rs.getInt(2));
    assertEquals("", rs.getString(3));
    assertEquals(
        "The name of the user that the application using the connection is performing work for. "
            + "This may not be the same as the user name that was used in establishing the connection.",
        rs.getString(4));

    assertTrue(rs.next());
    assertEquals("ClientHostname", rs.getString(1));
    assertEquals(0x00ffffff, rs.getInt(2));
    assertEquals("", rs.getString(3));
    assertEquals("The hostname of the computer the application using the connection is running on",
        rs.getString(4));

    assertFalse(rs.next());
  }

  @Test
  public void getCatalogsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getCatalogs(), "TABLE_CAT String");
  }

  @Test
  public void getColumnsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getColumns(null, null, null, null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
            + "DATA_TYPE int,TYPE_NAME String,COLUMN_SIZE decimal,BUFFER_LENGTH int,"
            + "DECIMAL_DIGITS int,NUM_PREC_RADIX int,NULLABLE int,"
            + "REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
            + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,"
            + "ORDINAL_POSITION int,IS_NULLABLE String,"
            + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
            + "SCOPE_TABLE String,SOURCE_DATA_TYPE null");
  }

  @Test
  public void getProcedureColumnsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getProcedureColumns(null, null, null, null),
        "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,COLUMN_NAME String ,"
            + "COLUMN_TYPE short,DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,"
            + "RADIX short,NULLABLE short,REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
            + "SQL_DATETIME_SUB int ,CHAR_OCTET_LENGTH int,"
            + "ORDINAL_POSITION int,IS_NULLABLE String,SPECIFIC_NAME String");
  }

  @Test
  public void getFunctionColumnsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getFunctionColumns(null, null, null, null),
        "FUNCTION_CAT String,FUNCTION_SCHEM String,FUNCTION_NAME String,COLUMN_NAME String,COLUMN_TYPE short,"
            + "DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE short,RADIX short,"
            + "NULLABLE short,REMARKS String,CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,"
            + "IS_NULLABLE String,SPECIFIC_NAME String");
  }

  @Test
  public void getColumnPrivilegesBasic() throws SQLException {
    Common.assertThrowsContains(SQLException.class,
        () -> sharedConn.getMetaData().getColumnPrivileges(null, null, null, null),
        "'table' parameter must not be null");
    testResultSetColumns(sharedConn.getMetaData().getColumnPrivileges(null, null, "", null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
            + "GRANTOR String,GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
  }

  @Test
  public void getTablePrivilegesBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getTablePrivileges(null, null, null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,GRANTOR String,"
            + "GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
  }

  @Test
  public void getVersionColumnsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getVersionColumns(null, null, null),
        "SCOPE short, COLUMN_NAME String,DATA_TYPE int,TYPE_NAME String,"
            + "COLUMN_SIZE int,BUFFER_LENGTH int,DECIMAL_DIGITS short," + "PSEUDO_COLUMN short");
  }

  @Test
  public void getPrimaryKeysBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getPrimaryKeys(null, null, null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,KEY_SEQ short,PK_NAME String");
  }

  @Test
  public void getImportedKeysBasic() throws SQLException {
    ResultSet rs = sharedConn.getMetaData().getImportedKeys(null, null, "");
    assertFalse(rs.next());
  }

  @Test
  public void getExportedKeysBasic() throws SQLException {
    ResultSet rs = sharedConn.getMetaData().getImportedKeys(null, null, "");
    assertFalse(rs.next());
  }

  @Test
  public void getCrossReferenceResults() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    try {
      dbmd.getCrossReference(null, null, "cross%", null, null, "cross%");
      fail("Should have thrown an SQLFeatureNotSupportedException exception");
    } catch (SQLFeatureNotSupportedException ignored) {
    }
  }

  @Test
  public void getUdtsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getUDTs(null, null, null, null),
        "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,CLASS_NAME String,DATA_TYPE int,"
            + "REMARKS String,BASE_TYPE short");
  }

  @Test
  public void getSuperTypesBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getSuperTypes(null, null, null),
        "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,SUPERTYPE_CAT String,"
            + "SUPERTYPE_SCHEM String,SUPERTYPE_NAME String");
  }

  @Test
  public void getFunctionsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getFunctions(null, null, null),
        "FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS String,FUNCTION_TYPE short, "
            + "SPECIFIC_NAME String");
  }

  @Test
  public void getFunctionType() throws SQLException {
    ResultSet rs = sharedConn.getMetaData().getFunctions(null, null, "UDTF");
    assertTrue(rs.next());
    assertEquals(DatabaseMetaData.functionReturnsTable, rs.getInt("FUNCTION_TYPE"));
    assertFalse(rs.next());
    rs = sharedConn.getMetaData().getFunctions(null, null, "testMetaCatalog");
    assertTrue(rs.next());
    assertEquals(DatabaseMetaData.functionNoTable, rs.getInt("FUNCTION_TYPE"));
    assertFalse(rs.next());
  }

  @Test
  public void getFunctionsUDAF() throws SQLException {
    ResultSet rs = sharedConn.getMetaData().getFunctions(null, null, "UDAF");
    assertTrue(rs.next());

    assertEquals(sharedConn.getCatalog(), rs.getString("FUNCTION_CAT"));
    assertNull(rs.getString("FUNCTION_SCHEM"));
    assertEquals("UDAF", rs.getString("FUNCTION_NAME"));
    assertNull(rs.getString("REMARKS"));
    assertEquals(DatabaseMetaData.functionNoTable, rs.getInt("FUNCTION_TYPE"));
    assertEquals("UDAF", rs.getString("SPECIFIC_NAME"));

    assertFalse(rs.next());

    rs = sharedConn.getMetaData().getFunctions(null, null, "UDAF%");
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertTrue(rs.next());
    assertFalse(rs.next());
  }

  @Test
  public void getProceduresNotFunctions() throws SQLException {
    ResultSet rs;
    rs = sharedConn.getMetaData().getProcedures(null, null, "getProcTimePrecision");
    assertTrue(rs.next());
    rs = sharedConn.getMetaData().getProcedures(null, null, "getProcTimePrecision2");
    assertTrue(rs.next());
    rs = sharedConn.getMetaData().getProcedures(null, null, "testMetaCatalogProc");
    assertTrue(rs.next());
    rs = sharedConn.getMetaData().getProcedures(null, null, "testMetaCatalog");
    assertFalse(rs.next());
    rs = sharedConn.getMetaData().getProcedures(null, null, "UDTF");
    assertFalse(rs.next());
  }

  @Test
  public void getSuperTablesBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getSuperTables(null, null, null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String, SUPERTABLE_NAME String");
  }

  @Test
  public void testGetSchemas2() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getCatalogs();
    boolean foundTestUnitsJdbc = false;
    while (rs.next()) {
      if (rs.getString(1).equals(sharedConn.getCatalog())) {
        foundTestUnitsJdbc = true;
      }
    }
    assertEquals(true, foundTestUnitsJdbc);
  }

  /* Verify default behavior for nullCatalogMeansCurrent (=true) */
  @Test
  public void nullCatalogMeansCurrent() throws Exception {
    String catalog = sharedConn.getCatalog();
    ResultSet rs = sharedConn.getMetaData().getColumns(null, null, null, null);
    while (rs.next()) {
      assertTrue(rs.getString("TABLE_CAT").equalsIgnoreCase(catalog));
    }
  }

  @Test
  public void testGetTypeInfoBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getTypeInfo(),
        "TYPE_NAME String,DATA_TYPE int,PRECISION int,LITERAL_PREFIX String,"
            + "LITERAL_SUFFIX String,CREATE_PARAMS String, NULLABLE short,CASE_SENSITIVE boolean,"
            + "SEARCHABLE short,UNSIGNED_ATTRIBUTE boolean,FIXED_PREC_SCALE boolean, "
            + "AUTO_INCREMENT boolean, LOCAL_TYPE_NAME String,MINIMUM_SCALE short,MAXIMUM_SCALE short,"
            + "SQL_DATA_TYPE int,SQL_DATETIME_SUB int, NUM_PREC_RADIX int");
  }

  @Test
  public void getColumnsTest() throws SQLException {

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getColumns(sharedConn.getCatalog(), null, "manycols", null);
    assertTrue(rs.next());
    do {
      String columnName = rs.getString("column_name");
      int type = rs.getInt("data_type");
      String typeName = rs.getString("type_name");
      for (char c : typeName.toCharArray()) {
        assertTrue(c == ' ' || Character.isUpperCase(c) || !Character.isAlphabetic(c),
            "bad typename " + typeName);
      }
      checkType(columnName, type, "tiny", Types.BIT);
      checkType(columnName, type, "tiny_uns", Types.BIT);
      checkType(columnName, type, "small", Types.SMALLINT);
      checkType(columnName, type, "small_uns", Types.SMALLINT);
      checkType(columnName, type, "medium", Types.INTEGER);
      checkType(columnName, type, "medium_uns", Types.INTEGER);
      checkType(columnName, type, "int_col", Types.INTEGER);
      checkType(columnName, type, "int_col_uns", Types.INTEGER);
      checkType(columnName, type, "big", Types.BIGINT);
      checkType(columnName, type, "big_uns", Types.BIGINT);
      checkType(columnName, type, "decimal_col", Types.DECIMAL);
      checkType(columnName, type, "fcol", Types.REAL);
      checkType(columnName, type, "fcol_uns", Types.REAL);
      checkType(columnName, type, "dcol", Types.DOUBLE);
      checkType(columnName, type, "dcol_uns", Types.DOUBLE);
      checkType(columnName, type, "date_col", Types.DATE);
      checkType(columnName, type, "time_col", Types.TIME);
      checkType(columnName, type, "timestamp_col", Types.TIMESTAMP);
      checkType(columnName, type, "year_col", Types.DATE);
      checkType(columnName, type, "bit_col", Types.BIT);
      checkType(columnName, type, "char_col", Types.CHAR);
      checkType(columnName, type, "varchar_col", Types.VARCHAR);
      checkType(columnName, type, "binary_col", Types.BINARY);
      checkType(columnName, type, "tinyblob_col", Types.VARBINARY);
      checkType(columnName, type, "blob_col", Types.LONGVARBINARY);
      checkType(columnName, type, "longblob_col", Types.LONGVARBINARY);
      checkType(columnName, type, "mediumblob_col", Types.LONGVARBINARY);
      checkType(columnName, type, "tinytext_col", Types.VARCHAR);
      checkType(columnName, type, "text_col", Types.LONGVARCHAR);
      checkType(columnName, type, "mediumtext_col", Types.LONGVARCHAR);
      checkType(columnName, type, "longtext_col", Types.LONGVARCHAR);
    } while (rs.next());
  }

  @Test
  public void yearIsShortType() throws Exception {
    try (java.sql.Connection connection = createCon("&yearIsDateType=false")) {
      connection.createStatement().execute("insert into ytab values(72)");

      ResultSet rs2 =
          connection.getMetaData().getColumns(connection.getCatalog(), null, "ytab", null);
      assertTrue(rs2.next());
      assertEquals(Types.SMALLINT, rs2.getInt("DATA_TYPE"));

      try (ResultSet rs =
          connection.getMetaData().getColumns(connection.getCatalog(), null, "ytab", null)) {
        assertTrue(rs.next());
        assertEquals(Types.SMALLINT, rs.getInt("DATA_TYPE"));
      }

      try (ResultSet rs1 = connection.createStatement().executeQuery("select * from ytab")) {
        assertEquals(rs1.getMetaData().getColumnType(1), Types.SMALLINT);
        assertTrue(rs1.next());
        assertTrue(rs1.getObject(1) instanceof Short);
        assertEquals(rs1.getShort(1), 1972);
      }
    }
  }

  @Test
  public void yearIsDateType() throws Exception {
    try (Connection connection = createCon("&yearIsDateType=true")) {
      connection.createStatement().execute("insert into ytab values(72)");

      ResultSet rs2 =
          connection.getMetaData().getColumns(connection.getCatalog(), null, "ytab", null);
      assertTrue(rs2.next());
      assertEquals(Types.DATE, rs2.getInt("DATA_TYPE"));

      try (ResultSet rs =
          connection.getMetaData().getColumns(connection.getCatalog(), null, "ytab", null)) {
        assertTrue(rs.next());
        assertEquals(Types.DATE, rs.getInt("DATA_TYPE"));
      }

      try (ResultSet rs1 = connection.createStatement().executeQuery("select * from ytab")) {
        assertEquals(Types.DATE, rs1.getMetaData().getColumnType(1));
        assertTrue(rs1.next());
        assertTrue(rs1.getObject(1) instanceof Date);
        assertEquals("1972-01-01", rs1.getDate(1).toString());
      }
    }
  }

  /* CONJ-15 */
  @Test
  public void maxCharLengthUtf8() throws Exception {
    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getColumns(sharedConn.getCatalog(), null, "maxcharlength", null);
    assertTrue(rs.next());
    assertEquals(rs.getInt("COLUMN_SIZE"), 1);
  }

  @Test
  public void conj72() throws Exception {
    try (Connection connection = createCon("&tinyInt1isBit=true")) {
      connection.createStatement().execute("insert into conj72 values(1)");
      ResultSet rs =
          connection.getMetaData().getColumns(connection.getCatalog(), null, "conj72", null);
      assertTrue(rs.next());
      assertEquals(rs.getInt("DATA_TYPE"), Types.BIT);
      ResultSet rs1 = connection.createStatement().executeQuery("select * from conj72");
      assertEquals(rs1.getMetaData().getColumnType(1), Types.BIT);
    }
  }

  @Test
  public void getPrecision() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS getPrecision(" + "num1 NUMERIC(9,4), " + "num2 NUMERIC (9,0),"
            + "num3 NUMERIC (9,4) UNSIGNED," + "num4 NUMERIC (9,0) UNSIGNED," + "num5 FLOAT(9,4),"
            + "num6 FLOAT(9,4) UNSIGNED," + "num7 DOUBLE(9,4)," + "num8 DOUBLE(9,4) UNSIGNED)");
    ResultSet rs = stmt.executeQuery("SELECT * FROM getPrecision");
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals(9, rsmd.getPrecision(1));
    assertEquals(4, rsmd.getScale(1));
    assertEquals(9, rsmd.getPrecision(2));
    assertEquals(0, rsmd.getScale(2));
    assertEquals(9, rsmd.getPrecision(3));
    assertEquals(4, rsmd.getScale(3));
    assertEquals(9, rsmd.getPrecision(4));
    assertEquals(0, rsmd.getScale(4));
    assertEquals(12, rsmd.getPrecision(5));
    assertEquals(4, rsmd.getScale(5));
    assertEquals(12, rsmd.getPrecision(6));
    assertEquals(4, rsmd.getScale(6));
    assertEquals(22, rsmd.getPrecision(7));
    assertEquals(4, rsmd.getScale(7));
    assertEquals(22, rsmd.getPrecision(8));
    assertEquals(4, rsmd.getScale(8));
  }

  @Test
  public void getTimePrecision() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM getTimePrecision");
    ResultSetMetaData rsmd = rs.getMetaData();
    // date
    assertEquals(10, rsmd.getPrecision(1));
    assertEquals(0, rsmd.getScale(1));
    // datetime(0)
    assertEquals(19, rsmd.getPrecision(2));
    assertEquals(0, rsmd.getScale(2));
    // datetime(6)
    assertEquals(26, rsmd.getPrecision(3));
    assertEquals(6, rsmd.getScale(3));
    // timestamp(0)
    assertEquals(19, rsmd.getPrecision(4));
    assertEquals(0, rsmd.getScale(4));
    // timestamp(6)
    assertEquals(26, rsmd.getPrecision(5));
    assertEquals(6, rsmd.getScale(5));
    // time(0)
    assertEquals(10, rsmd.getPrecision(6));
    assertEquals(0, rsmd.getScale(6));
    // time(6)
    assertEquals(17, rsmd.getPrecision(7));
    assertEquals(6, rsmd.getScale(7));
  }

  @Test
  public void metaTimeResultSet() throws SQLException {
    final int columnSizeField = 7;

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getColumns(null, null, "getTimePrecision", null);
    // date
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(columnSizeField));
    // datetime(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(columnSizeField));
    // datetime(6)
    assertTrue(rs.next());
    assertEquals(26, rs.getInt(columnSizeField));
    // timestamp(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(columnSizeField));
    // timestamp(6)
    assertTrue(rs.next());
    assertEquals(26, rs.getInt(columnSizeField));
    // time(0)
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(columnSizeField));
    // time(6)
    assertTrue(rs.next());
    assertEquals(17, rs.getInt(columnSizeField));

    assertFalse(rs.next());
  }

  /**
   * CONJ-401 - getProcedureColumns precision when server doesn't support precision.
   *
   * @throws SQLException if connection error occur
   */
  @Test
  public void metaTimeNoPrecisionProcedureResultSet() throws SQLException {
    Statement stmt = sharedConn.createStatement();

    final int precisionField = 8;
    final int lengthField = 9;
    final int scaleField = 10;

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getProcedureColumns(null, null, "getProcTimePrecision2", null);
    // return type
    assertTrue(rs.next());
    // date
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(precisionField));
    assertEquals(10, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    assertTrue(rs.wasNull());
    // datetime(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(precisionField));
    assertEquals(19, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    // timestamp(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(precisionField));
    assertEquals(19, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    // time(0)
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(precisionField));
    assertEquals(10, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));

    assertFalse(rs.next());
  }

  /**
   * CONJ-381 - getProcedureColumns returns NULL as TIMESTAMP/DATETIME precision instead of 19.
   *
   * @throws SQLException if connection error occur
   */
  @Test
  public void metaTimeProcedureResultSet() throws SQLException {
    Statement stmt = sharedConn.createStatement();

    final int precisionField = 8;
    final int lengthField = 9;
    final int scaleField = 10;

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getProcedureColumns(null, null, "getProcTimePrecision", null);
    // return type
    assertTrue(rs.next());
    // date
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(precisionField));
    assertEquals(10, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    assertTrue(rs.wasNull());
    // datetime(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(precisionField));
    assertEquals(19, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    // datetime(6)
    assertTrue(rs.next());
    assertEquals(26, rs.getInt(precisionField));
    assertEquals(26, rs.getInt(lengthField));
    assertEquals(6, rs.getInt(scaleField));
    // timestamp(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(precisionField));
    assertEquals(19, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    // timestamp(6)
    assertTrue(rs.next());
    assertEquals(26, rs.getInt(precisionField));
    assertEquals(26, rs.getInt(lengthField));
    assertEquals(6, rs.getInt(scaleField));
    // time(0)
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(precisionField));
    assertEquals(10, rs.getInt(lengthField));
    assertEquals(0, rs.getInt(scaleField));
    // time(6)
    assertTrue(rs.next());
    assertEquals(17, rs.getInt(precisionField));
    assertEquals(17, rs.getInt(lengthField));
    assertEquals(6, rs.getInt(scaleField));

    assertFalse(rs.next());
  }

  @Test
  public void various() throws SQLException {
    com.singlestore.jdbc.DatabaseMetaData meta = sharedConn.getMetaData();
    assertEquals(minVersion(8, 5, 0) ? 256 : 64, meta.getMaxProcedureNameLength());
  }

  @Test
  public void getIndexInfo() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    DatabaseMetaData meta = sharedConn.getMetaData();

    Common.assertThrowsContains(SQLException.class,
        () -> meta.getIndexInfo(null, null, null, true, true),
        "'table' parameter must not be null");

    ResultSet rs = meta.getIndexInfo(null, null, "get_index_info", false, true);
    rs.next();
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("get_index_info", rs.getString(3));
    assertFalse(rs.getBoolean(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertEquals("PRIMARY", rs.getString(6));
    assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
    assertEquals(1, rs.getShort(8));
    assertEquals("no", rs.getString(9));
    assertNull(rs.getString(10));
    assertEquals(0l, rs.getLong(11));
    assertNull(rs.getString(12));
    assertNull(rs.getString(13));

    if (!minVersion(7, 5, 0)) {
      assertTrue(rs.next());
      assertEquals(sharedConn.getCatalog(), rs.getString(1));
      assertNull(rs.getString(2));
      assertEquals("get_index_info", rs.getString(3));
      assertFalse(rs.getBoolean(4));
      assertEquals(sharedConn.getCatalog(), rs.getString(5));
      assertEquals("PRIMARY", rs.getString(6));
      assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
      assertEquals(1, rs.getShort(8));
      assertEquals("no", rs.getString(9));
      assertNull(rs.getString(10));
      assertEquals(0l, rs.getLong(11));
      assertNull(rs.getString(12));
      assertNull(rs.getString(13));
    }

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("get_index_info", rs.getString(3));
    assertTrue(rs.getBoolean(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertEquals("ind_cust", rs.getString(6));
    assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
    assertEquals(1, rs.getShort(8));
    assertEquals("customer_id", rs.getString(9));
    assertNull(rs.getString(10));
    assertEquals(0l, rs.getLong(11));
    assertNull(rs.getString(12));
    assertNull(rs.getString(13));

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("get_index_info", rs.getString(3));
    assertTrue(rs.getBoolean(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertEquals("ind_prod", rs.getString(6));
    assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
    assertEquals(1, rs.getShort(8));
    assertEquals("product_category", rs.getString(9));
    assertNull(rs.getString(10));
    assertEquals(0l, rs.getLong(11));
    assertNull(rs.getString(12));
    assertNull(rs.getString(13));

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("get_index_info", rs.getString(3));
    assertTrue(rs.getBoolean(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertEquals("ind_prod", rs.getString(6));
    assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
    assertEquals(2, rs.getShort(8));
    assertEquals("product_id", rs.getString(9));
    assertNull(rs.getString(10));
    assertEquals(0l, rs.getLong(11));
    assertNull(rs.getString(12));
    assertNull(rs.getString(13));
  }

  @Test
  public void getPseudoColumns() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getPseudoColumns(null, null, null, null);
    assertFalse(rs.next());
  }

  @Test
  public void constantTest() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    assertFalse(meta.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
    assertFalse(meta.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
    assertFalse(meta.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
    assertFalse(meta.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
    assertFalse(meta.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
    assertFalse(meta.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
    assertTrue(meta.supportsBatchUpdates());
    assertEquals(sharedConn, sharedConn.getMetaData().getConnection());
    assertFalse(meta.supportsSavepoints());
    assertFalse(meta.supportsNamedParameters());
    assertFalse(meta.supportsMultipleOpenResults());
    assertTrue(meta.supportsGetGeneratedKeys());
    assertTrue(meta.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertFalse(meta.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, meta.getResultSetHoldability());
    assertTrue(meta.getDatabaseMajorVersion() >= 5);
    assertTrue(meta.getDatabaseMinorVersion() >= 0);
    assertEquals(4, meta.getJDBCMajorVersion());
    assertEquals(2, meta.getJDBCMinorVersion());
    assertEquals(DatabaseMetaData.sqlStateSQL99, meta.getSQLStateType());
    assertFalse(meta.locatorsUpdateCopy());
    assertFalse(meta.supportsStatementPooling());
    assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, meta.getRowIdLifetime());
    assertTrue(meta.supportsStoredFunctionsUsingCallSyntax());
    assertFalse(meta.autoCommitFailureClosesAllResultSets());

    meta.unwrap(java.sql.DatabaseMetaData.class);
    Common.assertThrowsContains(SQLException.class, () -> meta.unwrap(String.class),
        "The receiver is not a wrapper for java.lang.String");

    assertEquals(4294967295L, meta.getMaxLogicalLobSize());
    assertFalse(meta.supportsRefCursors());
    assertTrue(meta.supportsGetGeneratedKeys());
    assertTrue(meta.generatedKeyAlwaysReturned());
    assertTrue(meta.allProceduresAreCallable());
    assertTrue(meta.allTablesAreSelectable());
    assertNotNull(meta.getURL());
    assertNotNull(meta.getUserName());
    assertFalse(meta.isReadOnly());
    assertFalse(meta.nullsAreSortedHigh());
    assertTrue(meta.nullsAreSortedLow());
    assertFalse(meta.nullsAreSortedAtStart());
    assertTrue(meta.nullsAreSortedAtEnd());
    assertEquals("SingleStore", meta.getDatabaseProductName());
    assertEquals("SingleStore JDBC", meta.getDriverName());
    assertTrue(meta.getDriverVersion().startsWith("1."));
    assertTrue(meta.getDriverMajorVersion() >= 0);
    assertTrue(meta.getDriverMinorVersion() >= 0);
    assertFalse(meta.usesLocalFiles());
    assertFalse(meta.usesLocalFilePerTable());
    assertEquals(meta.supportsMixedCaseIdentifiers(), meta.supportsMixedCaseQuotedIdentifiers());
    assertEquals(meta.storesUpperCaseIdentifiers(), meta.storesUpperCaseQuotedIdentifiers());
    assertEquals(meta.storesLowerCaseIdentifiers(), meta.storesLowerCaseQuotedIdentifiers());
    assertEquals(meta.storesMixedCaseIdentifiers(), meta.storesMixedCaseQuotedIdentifiers());
    assertEquals("`", meta.getIdentifierQuoteString());
    assertNotNull(meta.getSQLKeywords());
    assertNotNull(meta.getNumericFunctions());
    assertNotNull(meta.getStringFunctions());
    assertNotNull(meta.getSystemFunctions());
    assertNotNull(meta.getTimeDateFunctions());
    assertEquals("\\", meta.getSearchStringEscape());
    assertEquals("#@", meta.getExtraNameCharacters());
    assertTrue(meta.supportsAlterTableWithAddColumn());
    assertTrue(meta.supportsAlterTableWithDropColumn());
    assertTrue(meta.supportsColumnAliasing());
    assertTrue(meta.nullPlusNonNullIsNull());
    assertTrue(meta.supportsConvert());
    assertTrue(meta.supportsAlterTableWithAddColumn());
    assertTrue(meta.supportsConvert(Types.INTEGER, Types.REAL));
    assertFalse(meta.supportsConvert(Types.INTEGER, Types.BLOB));
    assertTrue(meta.supportsConvert(Types.BLOB, Types.TINYINT));
    assertFalse(meta.supportsConvert(Types.BLOB, Types.ARRAY));
    assertTrue(meta.supportsConvert(Types.CLOB, Types.NUMERIC));
    assertFalse(meta.supportsConvert(Types.CLOB, Types.ARRAY));
    assertTrue(meta.supportsConvert(Types.DATE, Types.VARCHAR));
    assertFalse(meta.supportsConvert(Types.DATE, Types.ARRAY));
    assertTrue(meta.supportsConvert(Types.TIME, Types.VARCHAR));
    assertFalse(meta.supportsConvert(Types.TIME, Types.ARRAY));
    assertTrue(meta.supportsConvert(Types.TIMESTAMP, Types.VARCHAR));
    assertFalse(meta.supportsConvert(Types.TIMESTAMP, Types.ARRAY));
    assertFalse(meta.supportsConvert(Types.ARRAY, Types.TIMESTAMP));
    assertTrue(meta.supportsTableCorrelationNames());
    assertTrue(meta.supportsDifferentTableCorrelationNames());
    assertTrue(meta.supportsExpressionsInOrderBy());
    assertTrue(meta.supportsOrderByUnrelated());
    assertTrue(meta.supportsGroupBy());
    assertTrue(meta.supportsGroupByUnrelated());
    assertTrue(meta.supportsGroupByBeyondSelect());
    assertTrue(meta.supportsLikeEscapeClause());
    assertTrue(meta.supportsMultipleResultSets());
    assertTrue(meta.supportsMultipleTransactions());
    assertTrue(meta.supportsNonNullableColumns());
    assertTrue(meta.supportsMinimumSQLGrammar());
    assertTrue(meta.supportsCoreSQLGrammar());
    assertTrue(meta.supportsExtendedSQLGrammar());
    assertTrue(meta.supportsANSI92EntryLevelSQL());
    assertTrue(meta.supportsANSI92IntermediateSQL());
    assertTrue(meta.supportsANSI92FullSQL());
    assertTrue(meta.supportsIntegrityEnhancementFacility());
    assertTrue(meta.supportsOuterJoins());
    assertTrue(meta.supportsFullOuterJoins());
    assertTrue(meta.supportsLimitedOuterJoins());
    assertEquals("schema", meta.getSchemaTerm());
    assertEquals("procedure", meta.getProcedureTerm());
    assertEquals("database", meta.getCatalogTerm());
    assertTrue(meta.isCatalogAtStart());
    assertEquals(".", meta.getCatalogSeparator());
    assertFalse(meta.supportsSchemasInDataManipulation());
    assertFalse(meta.supportsSchemasInProcedureCalls());
    assertFalse(meta.supportsSchemasInTableDefinitions());
    assertFalse(meta.supportsSchemasInIndexDefinitions());
    assertFalse(meta.supportsSchemasInPrivilegeDefinitions());
    assertTrue(meta.supportsCatalogsInDataManipulation());
    assertTrue(meta.supportsCatalogsInProcedureCalls());
    assertTrue(meta.supportsCatalogsInTableDefinitions());
    assertTrue(meta.supportsCatalogsInIndexDefinitions());
    assertTrue(meta.supportsCatalogsInPrivilegeDefinitions());
    assertFalse(meta.supportsPositionedDelete());
    assertFalse(meta.supportsPositionedUpdate());
    assertTrue(meta.supportsSelectForUpdate());
    assertTrue(meta.supportsStoredProcedures());
    assertTrue(meta.supportsSubqueriesInComparisons());
    assertTrue(meta.supportsSubqueriesInExists());
    assertTrue(meta.supportsSubqueriesInIns());
    assertTrue(meta.supportsSubqueriesInComparisons());
    assertTrue(meta.supportsSubqueriesInQuantifieds());
    assertTrue(meta.supportsCorrelatedSubqueries());
    assertTrue(meta.supportsUnion());
    assertTrue(meta.supportsUnionAll());
    assertTrue(meta.supportsOpenCursorsAcrossCommit());
    assertTrue(meta.supportsOpenCursorsAcrossRollback());
    assertTrue(meta.supportsOpenStatementsAcrossCommit());
    assertTrue(meta.supportsOpenStatementsAcrossRollback());
    assertEquals(Integer.MAX_VALUE, meta.getMaxBinaryLiteralLength());
    assertEquals(Integer.MAX_VALUE, meta.getMaxCharLiteralLength());
    assertEquals(minVersion(8, 5, 0) ? 256 : 64, meta.getMaxColumnNameLength());
    assertEquals(64, meta.getMaxColumnsInGroupBy());
    assertEquals(16, meta.getMaxColumnsInIndex());
    assertEquals(64, meta.getMaxColumnsInOrderBy());
    assertEquals(Short.MAX_VALUE, meta.getMaxColumnsInSelect());
    assertEquals(0, meta.getMaxColumnsInTable());
    assertEquals(0, meta.getMaxConnections());
    assertEquals(0, meta.getMaxCursorNameLength());
    assertEquals(256, meta.getMaxIndexLength());
    assertEquals(0, meta.getMaxSchemaNameLength());
    assertEquals(minVersion(8, 5, 0) ? 256 : 64, meta.getMaxProcedureNameLength());
    assertEquals(0, meta.getMaxCatalogNameLength());
    assertEquals(0, meta.getMaxRowSize());
    assertFalse(meta.doesMaxRowSizeIncludeBlobs());
    assertEquals(0, meta.getMaxStatementLength());
    assertEquals(0, meta.getMaxStatements());
    assertEquals(minVersion(8, 5, 0) ? 256 : 64, meta.getMaxTableNameLength());
    assertEquals(256, meta.getMaxTablesInSelect());
    assertEquals(0, meta.getMaxUserNameLength());
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, meta.getDefaultTransactionIsolation());
    assertTrue(meta.supportsTransactions());
    assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
    assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
    assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
    assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
    assertTrue(meta.supportsDataDefinitionAndDataManipulationTransactions());
    assertFalse(meta.supportsDataManipulationTransactionsOnly());
    assertTrue(meta.dataDefinitionCausesTransactionCommit());
    assertFalse(meta.dataDefinitionIgnoredInTransactions());
  }

  @Test
  public void testMetaCatalog() throws Exception {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getProcedures(sharedConn.getCatalog(), null, "testMetaCatalogProc");
    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("testMetaCatalogProc", rs.getString(3));
    assertNull(rs.getString(4));
    assertNull(rs.getString(5));
    assertNull(rs.getString(6));
    assertEquals(DatabaseMetaData.procedureReturnsResult, rs.getInt(8));
    assertEquals("testMetaCatalogProc", rs.getString(9));
    assertFalse(rs.next());

    // test with bad catalog
    rs = meta.getProcedures("yahoooo", null, "testMetaCatalogProc");
    assertFalse(rs.next());

    // test without catalog
    rs = meta.getProcedures(null, null, "testMetaCatalogProc");
    assertTrue(rs.next());
    assertEquals("testMetaCatalogProc", rs.getString(3));
    assertFalse(rs.next());
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = sharedConn.createStatement().executeQuery("SELECT * FROM json_test");
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("JSON", meta.getColumnTypeName(1));
    assertEquals("java.lang.String", meta.getColumnClassName(1));
    assertEquals(Types.LONGVARCHAR, meta.getColumnType(1));
  }
}
