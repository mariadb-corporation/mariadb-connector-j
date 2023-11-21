// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Statement;

public class DatabaseMetadataTest extends Common {

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS getProcTimePrecision");
    stmt.execute("DROP PROCEDURE IF EXISTS getProcTimePrecision2");
    stmt.execute("DROP PROCEDURE IF EXISTS testMetaCatalog");
    stmt.execute("DROP TABLE IF EXISTS json_test");
    stmt.execute("DROP TABLE IF EXISTS dbpk_test");
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
    stmt.execute("drop table if exists cross3");
    stmt.execute("drop table if exists cross2");
    stmt.execute("drop table if exists cross1");
    stmt.execute("drop table if exists get_index_info");
    stmt.execute("drop table if exists text_types_text");
  }

  @BeforeAll
  public static void initClass() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS getTimePrecision("
            + "d date, "
            + "t1 datetime(0),"
            + "t2 datetime(6),"
            + "t3 timestamp(0) DEFAULT '2000-01-01 00:00:00',"
            + "t4 timestamp(6) DEFAULT '2000-01-01 00:00:00',"
            + "t5 time(0),"
            + "t6 time(6))");
    stmt.execute("CREATE TABLE json_test(t1 JSON)");
    stmt.execute(
        "CREATE PROCEDURE testMetaCatalog(x int, out y int) COMMENT 'comments' \n"
            + "BEGIN\n"
            + "SELECT 1;end\n");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS dbpk_test(val varchar(20), id1 int not null, id2 int not"
            + " null,primary key(id1, id2)) engine=innodb");
    stmt.execute("CREATE TABLE IF NOT EXISTS datetime_test(dt datetime)");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS `manycols`(  `tiny` tinyint(4) DEFAULT NULL,  `tiny_uns`"
            + " tinyint(3) unsigned DEFAULT NULL,  `small` smallint(6) DEFAULT NULL,  `small_uns`"
            + " smallint(5) unsigned DEFAULT NULL,  `medium` mediumint(9) DEFAULT NULL, "
            + " `medium_uns` mediumint(8) unsigned DEFAULT NULL,  `int_col` int(11) DEFAULT NULL, "
            + " `int_col_uns` int(10) unsigned DEFAULT NULL,  `big` bigint(20) DEFAULT NULL, "
            + " `big_uns` bigint(20) unsigned DEFAULT NULL,  `decimal_col` decimal(10,5) DEFAULT"
            + " NULL,  `fcol` float DEFAULT NULL,  `fcol_uns` float unsigned DEFAULT NULL,  `dcol`"
            + " double DEFAULT NULL,  `dcol_uns` double unsigned DEFAULT NULL,  `date_col` date"
            + " DEFAULT NULL,  `time_col` time DEFAULT NULL,  `timestamp_col` timestamp NOT NULL"
            + " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,  `year_col` year(4) DEFAULT"
            + " NULL,  `bit_col` bit(5) DEFAULT NULL,  `char_col` char(5) DEFAULT NULL, "
            + " `varchar_col` varchar(10) DEFAULT NULL,  `binary_col` binary(10) DEFAULT NULL, "
            + " `varbinary_col` varbinary(10) DEFAULT NULL,  `tinyblob_col` tinyblob,  `blob_col`"
            + " blob,  `mediumblob_col` mediumblob,  `longblob_col` longblob,  `tinytext_col`"
            + " tinytext,  `text_col` text,  `mediumtext_col` mediumtext,  `longtext_col`"
            + " longtext)");
    stmt.execute("CREATE TABLE IF NOT EXISTS ytab(y year)");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS maxcharlength(maxcharlength char(1)) character set utf8");
    stmt.execute("CREATE TABLE IF NOT EXISTS conj72(t tinyint(1))");
    if (isMariaDBServer() && minVersion(10, 3, 4)) {
      stmt.execute("CREATE TABLE IF NOT EXISTS versionTable(x INT) WITH SYSTEM VERSIONING");
    }
    stmt.execute("drop table if exists cross3");
    stmt.execute("drop table if exists cross2");
    stmt.execute("drop table if exists cross1");
    stmt.execute("create table cross1 (id int not null primary key, val varchar(20))");
    stmt.execute(
        "create table cross2 (id int not null, id2 int not null,  id_ref0 int, foreign key"
            + " (id_ref0) references cross1(id), UNIQUE unik_name (id, id2))");
    stmt.execute(
        "create table cross3 (id int not null primary key, id_ref1 int, id_ref2 int, foreign key"
            + " fk_my_name (id_ref1, id_ref2) references cross2(id, id2) on update cascade)");
    stmt.execute(
        "create table getBestRowIdentifier1(i int not null primary key auto_increment, id int,"
            + " id_ref1 int, id_ref2 int, foreign key fk_my_name_1 (id_ref1, id_ref2) references"
            + " cross2(id, id2) on update cascade, UNIQUE getBestRowIdentifier_unik (id))");
    stmt.execute(
        "create table getBestRowIdentifier2(id_ref0 int not null, id_ref1 int, id_ref2 int not"
            + " null, UNIQUE (id_ref1, id_ref2) , UNIQUE (id_ref0, id_ref2))");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS get_index_info(\n"
            + "    no INT NOT NULL AUTO_INCREMENT,\n"
            + "    product_category INT NOT NULL,\n"
            + "    product_id INT NOT NULL,\n"
            + "    customer_id INT NOT NULL,\n"
            + "    PRIMARY KEY(no),\n"
            + "    INDEX ind_prod (product_category, product_id),\n"
            + "    INDEX ind_cust (customer_id))");
    stmt.execute(
        "create table text_types_text (varchar100           varchar(100),\n"
            + "  varchar255           varchar(255),\n"
            + "  text                 text,\n"
            + "  `tinytext`           tinytext,\n"
            + "  `mediumtext`         mediumtext,\n"
            + "  `longtext`           longtext)");
  }

  private static void checkType(String name, int actualType, String colName, int expectedType) {
    if (name.equals(colName)) {
      assertEquals(actualType, expectedType);
    }
  }

  @Test
  public void metaUnsigned() throws SQLException {
    ResultSet typeInfo = sharedConn.getMetaData().getTypeInfo();
    ResultSetMetaData rsmd = typeInfo.getMetaData();
    assertEquals(Types.BOOLEAN, rsmd.getColumnType(typeInfo.findColumn("UNSIGNED_ATTRIBUTE")));
    while (typeInfo.next()) {
      Assertions.assertEquals(
          typeInfo.getString("TYPE_NAME").contains("UNSIGNED"),
          typeInfo.getBoolean("UNSIGNED_ATTRIBUTE"));
    }
  }

  @Test
  public void primaryKeysTest() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getPrimaryKeys(sharedConn.getCatalog(), null, "dbpk_test");
    int counter = 0;
    while (rs.next()) {
      counter++;
      assertEquals(sharedConn.getCatalog(), rs.getString("table_cat"));
      assertNull(rs.getString("table_schem"));
      assertEquals("dbpk_test", rs.getString("table_name"));
      assertEquals("id" + counter, rs.getString("column_name"));
      assertEquals("id" + counter, rs.getString("column_name"));
      assertEquals("PRIMARY", rs.getString("PK_NAME"));
    }
    assertEquals(2, counter);
  }

  @Test
  public void primaryKeysSchemaTest() throws SQLException {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      DatabaseMetaData meta = con.getMetaData();
      ResultSet rs = meta.getPrimaryKeys(null, database, "dbpk_test");
      int counter = 0;
      while (rs.next()) {
        counter++;
        assertEquals("def", rs.getString("table_cat"));
        assertEquals(database, rs.getString("table_schem"));
        assertEquals("dbpk_test", rs.getString("table_name"));
        assertEquals("id" + counter, rs.getString("column_name"));
        assertEquals("id" + counter, rs.getString("column_name"));
        assertEquals("PRIMARY", rs.getString("PK_NAME"));
      }
      assertEquals(2, counter);
    }
  }

  @Test
  public void primaryKeyTest2() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("drop table if exists primarykeytest2");
    stmt.execute("drop table if exists primarykeytest1");
    stmt.execute("CREATE TABLE primarykeytest1 ( id1 integer, constraint pk primary key(id1))");
    stmt.execute(
        "CREATE TABLE primarykeytest2 (id2a integer, id2b integer, constraint pk primary key(id2a,"
            + " id2b), constraint fk1 foreign key(id2a) references primarykeytest1(id1), "
            + " constraint fk2 foreign key(id2b) references primarykeytest1(id1))");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getPrimaryKeys(sharedConn.getCatalog(), null, "primarykeytest2");
    int counter = 0;
    while (rs.next()) {
      counter++;
      assertEquals(sharedConn.getCatalog(), rs.getString("table_cat"));
      assertNull(rs.getString("table_schem"));
      assertEquals("primarykeytest2", rs.getString("table_name"));
      assertEquals(counter, rs.getShort("key_seq"));
      assertEquals("PRIMARY", rs.getString("pk_name"));
    }
    assertEquals(2, counter);
    stmt.execute("drop table if exists primarykeytest2");
    stmt.execute("drop table if exists primarykeytest1");
  }

  @Test
  public void primaryKeyTest2Schema() throws SQLException {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("drop table if exists primarykeytest2");
      stmt.execute("drop table if exists primarykeytest1");
      stmt.execute("CREATE TABLE primarykeytest1 ( id1 integer, constraint pk primary key(id1))");
      stmt.execute(
          "CREATE TABLE primarykeytest2 (id2a integer, id2b integer, constraint pk primary"
              + " key(id2a, id2b), constraint fk1 foreign key(id2a) references"
              + " primarykeytest1(id1),  constraint fk2 foreign key(id2b) references"
              + " primarykeytest1(id1))");

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getPrimaryKeys(con.getCatalog(), null, "primarykeytest2");
      int counter = 0;
      while (rs.next()) {
        counter++;
        assertEquals("def", rs.getString("table_cat"));
        assertEquals(database, rs.getString("table_schem"));
        assertEquals("primarykeytest2", rs.getString("table_name"));
        assertEquals(counter, rs.getShort("key_seq"));
        assertEquals("PRIMARY", rs.getString("pk_name"));
      }
      assertEquals(2, counter);
      stmt.execute("drop table if exists primarykeytest2");
      stmt.execute("drop table if exists primarykeytest1");
    }
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

    if (meta.getDatabaseMajorVersion() < 5) {
      return;
    } else if (meta.getDatabaseMajorVersion() == 5 && meta.getDatabaseMinorVersion() < 5) {
      return;
    }

    stmt.execute("DROP FUNCTION IF EXISTS hello");
    stmt.execute(
        "CREATE FUNCTION hello (s CHAR(20), i int) RETURNS CHAR(50) DETERMINISTIC  "
            + "RETURN CONCAT('Hello, ',s,'!')");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs = meta.getFunctionColumns(null, null, "hello", null);

    assertTrue(rs.next());
    /* First row is for return value */
    assertEquals(rs.getString("FUNCTION_CAT"), sharedConn.getCatalog());
    assertNull(rs.getString("FUNCTION_SCHEM"));
    assertNull(rs.getString("COLUMN_NAME")); /* No name, since it is return value */
    assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionReturn);
    assertEquals(rs.getInt("DATA_TYPE"), Types.CHAR);
    assertEquals(rs.getString("TYPE_NAME"), "char");

    assertTrue(rs.next());
    assertEquals(rs.getString("COLUMN_NAME"), "s"); /* input parameter 's' (CHAR) */
    assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
    assertEquals(rs.getInt("DATA_TYPE"), Types.CHAR);
    assertEquals(rs.getString("TYPE_NAME"), "char");

    assertTrue(rs.next());
    assertEquals(rs.getString("COLUMN_NAME"), "i"); /* input parameter 'i' (INT) */
    assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
    assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
    assertEquals(rs.getString("TYPE_NAME"), "int");
    stmt.execute("DROP FUNCTION IF EXISTS hello");
  }

  @Test
  public void functionColumnsSchema() throws SQLException {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      java.sql.Statement stmt = con.createStatement();
      DatabaseMetaData meta = con.getMetaData();

      if (meta.getDatabaseMajorVersion() < 5) {
        return;
      } else if (meta.getDatabaseMajorVersion() == 5 && meta.getDatabaseMinorVersion() < 5) {
        return;
      }

      stmt.execute("DROP FUNCTION IF EXISTS hello");
      stmt.execute(
          "CREATE FUNCTION hello (s CHAR(20), i int) RETURNS CHAR(50) DETERMINISTIC  "
              + "RETURN CONCAT('Hello, ',s,'!')");
      stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
      ResultSet rs = meta.getFunctionColumns(null, null, "hello", null);

      assertTrue(rs.next());
      /* First row is for return value */
      assertEquals(rs.getString("FUNCTION_CAT"), "def");
      assertEquals(rs.getString("FUNCTION_SCHEM"), database);
      assertNull(rs.getString("COLUMN_NAME")); /* No name, since it is return value */
      assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionReturn);
      assertEquals(rs.getInt("DATA_TYPE"), Types.CHAR);
      assertEquals(rs.getString("TYPE_NAME"), "char");

      assertTrue(rs.next());
      assertEquals(rs.getString("COLUMN_NAME"), "s"); /* input parameter 's' (CHAR) */
      assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
      assertEquals(rs.getInt("DATA_TYPE"), Types.CHAR);
      assertEquals(rs.getString("TYPE_NAME"), "char");

      assertTrue(rs.next());
      assertEquals(rs.getString("COLUMN_NAME"), "i"); /* input parameter 'i' (INT) */
      assertEquals(rs.getInt("COLUMN_TYPE"), DatabaseMetaData.functionColumnIn);
      assertEquals(rs.getInt("DATA_TYPE"), Types.INTEGER);
      assertEquals(rs.getString("TYPE_NAME"), "int");
      stmt.execute("DROP FUNCTION IF EXISTS hello");
    }
  }

  /** Same as getImportedKeys, with one foreign key in a table in another catalog. */
  @Test
  public void getImportedKeys() throws Exception {
    getImportedKeys(sharedConn, true, true);
    try (org.mariadb.jdbc.Connection con = createCon()) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      getImportedKeys(con, true, true);
    }
    try (org.mariadb.jdbc.Connection con = createCon("importedKeysWithConstraintNames=false")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      getImportedKeys(con, false, true);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useCatalogTerm=Schema")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      getImportedKeys(con, true, false);
    }
    try (org.mariadb.jdbc.Connection con =
        createCon("importedKeysWithConstraintNames=false&useCatalogTerm=Schema")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      getImportedKeys(con, false, false);
    }
  }

  private void getImportedKeys(
      org.mariadb.jdbc.Connection con, boolean importedKeysWithConstraintNames, boolean useCatalog)
      throws Exception {
    // cancel for MySQL 8.0, since CASCADE with I_S give importedKeySetDefault, not
    // importedKeyCascade
    //    Assumptions.assumeFalse(!isMariaDBServer() && minVersion(8, 0, 0));
    Assumptions.assumeFalse(isXpand());
    java.sql.Statement st = con.createStatement();
    st.execute("USE " + database);
    st.execute("DROP TABLE IF EXISTS `product order 1`");
    st.execute("DROP TABLE IF EXISTS `other sch'ema`.`product order.2`");
    st.execute("DROP DATABASE IF EXISTS `other sch'ema`");
    st.execute("DROP TABLE IF EXISTS `product_order.3`");
    st.execute("DROP TABLE IF EXISTS product_order4");
    st.execute("DROP TABLE IF EXISTS t1.product ");
    st.execute("DROP TABLE IF EXISTS `cus``tomer`");
    st.execute("DROP DATABASE IF EXISTS test1");

    st.execute("CREATE DATABASE IF NOT EXISTS t1");
    st.execute("CREATE DATABASE IF NOT EXISTS `other sch'ema`");

    st.execute(
        "CREATE TABLE t1.product ( category INT NOT NULL, id INT NOT NULL, price DECIMAL,"
            + " UNIQUE unik_name (category, id) )");

    st.execute(
        "CREATE TABLE `cus``tomer` (id INT NOT NULL, id2 INT NOT NULL, PRIMARY KEY (id), UNIQUE"
            + " unikConst (id2))");
    String constraint = "ON UPDATE SET DEFAULT ON DELETE SET DEFAULT";
    if (!isMariaDBServer() || !minVersion(10, 5, 0))
      constraint = "ON UPDATE CASCADE ON DELETE CASCADE";
    st.execute(
        "CREATE TABLE `product order 1` (\n"
            + "    no INT NOT NULL AUTO_INCREMENT,\n"
            + "    product_category INT NOT NULL,\n"
            + "    product_id INT NOT NULL,\n"
            + "    customer_id INT DEFAULT NULL,\n"
            + "    PRIMARY KEY(no),\n"
            + "    INDEX (product_category, product_id),\n"
            + "    INDEX (customer_id),\n"
            + "    FOREIGN KEY (product_category, product_id)\n"
            + "      REFERENCES t1.product(category, id)\n"
            + "      ON UPDATE CASCADE ON DELETE CASCADE,\n"
            + "    FOREIGN KEY (customer_id)\n"
            + "      REFERENCES `cus``tomer`(id)\n"
            + constraint
            + ")");

    st.execute(
        "CREATE TABLE `other sch'ema`.`product order.2` (\n"
            + "    no INT NOT NULL,\n"
            + "    customer_id INT,\n"
            + "    FOREIGN KEY (customer_id)\n"
            + "      REFERENCES "
            + sharedConn.getCatalog()
            + ".`cus``tomer`(id)\n"
            + "      ON UPDATE RESTRICT ON DELETE RESTRICT)");

    st.execute(
        "CREATE TABLE `product_order.3` (\n"
            + "    no INT NOT NULL,\n"
            + "    customer_id INT,\n"
            + "    FOREIGN KEY (customer_id)\n"
            + "      REFERENCES `cus``tomer`(id)\n"
            + "      ON UPDATE SET NULL ON DELETE SET NULL)");

    st.execute(
        "CREATE TABLE product_order4 (\n"
            + "    no INT NOT NULL,\n"
            + "    customer_id INT,\n"
            + "    customer_id2 INT,\n"
            + "    FOREIGN KEY fk1 (customer_id)\n"
            + "      REFERENCES `cus``tomer`(id)\n"
            + "      ON UPDATE NO ACTION ON DELETE NO ACTION,"
            + "    FOREIGN KEY fk2 (customer_id2)\n"
            + "      REFERENCES `cus``tomer`(id2)\n"
            + "      ON UPDATE SET NULL ON DELETE SET NULL)");

    Common.assertThrowsContains(
        SQLException.class,
        () ->
            con.getMetaData()
                .getImportedKeys(useCatalog ? con.getCatalog() : con.getSchema(), null, null),
        "'table' parameter in getImportedKeys cannot be null");
    /*
    Test that I_S implementation is equivalent to parsing "show create table" .
     Get result sets using either method and compare (ignore minor differences INT vs SMALLINT
    */
    ResultSet rs1 =
        con.getMetaData()
            .getImportedKeysUsingShowCreateTable(
                useCatalog ? con.getCatalog() : con.getSchema(), "product order 1");
    ResultSet rs2 =
        con.getMetaData()
            .getImportedKeysUsingInformationSchema(
                useCatalog ? con.getCatalog() : con.getSchema(), "product order 1");
    assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());
    for (int i = 0; i < 2; i++) {
      ResultSet rs = i == 0 ? rs1 : rs2;
      assertTrue(rs.next());

      assertEquals(useCatalog ? "t1" : "def", rs.getString("PKTABLE_CAT"));
      assertEquals(useCatalog ? null : "t1", rs.getString("PKTABLE_SCHEM"));
      assertEquals("product", rs.getString("PKTABLE_NAME"));
      assertEquals("category", rs.getString("PKCOLUMN_NAME"));
      assertEquals(useCatalog ? con.getCatalog() : "def", rs.getString("FKTABLE_CAT"));
      assertEquals(useCatalog ? null : con.getSchema(), rs.getString("FKTABLE_SCHEM"));
      assertEquals("product order 1", rs.getString("FKTABLE_NAME"));
      assertEquals("product_category", rs.getString("FKCOLUMN_NAME"));
      assertEquals(1, rs.getInt("KEY_SEQ"));
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("DELETE_RULE"));
      assertEquals("product order 1_ibfk_1", rs.getString("FK_NAME"));
      // with show, meta don't know contraint name
      assertEquals(
          (i == 0 && !importedKeysWithConstraintNames) ? null : "unik_name",
          rs.getString("PK_NAME"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, rs.getInt("DEFERRABILITY"));

      assertTrue(rs.next());
      assertEquals(useCatalog ? "t1" : "def", rs.getString("PKTABLE_CAT"));
      assertEquals(useCatalog ? null : "t1", rs.getString("PKTABLE_SCHEM"));
      assertEquals("product", rs.getString("PKTABLE_NAME"));
      assertEquals("id", rs.getString("PKCOLUMN_NAME"));
      assertEquals(useCatalog ? con.getCatalog() : "def", rs.getString("FKTABLE_CAT"));
      assertEquals(useCatalog ? null : con.getSchema(), rs.getString("FKTABLE_SCHEM"));
      assertEquals("product order 1", rs.getString("FKTABLE_NAME"));
      assertEquals("product_id", rs.getString("FKCOLUMN_NAME"));
      assertEquals(2, rs.getInt("KEY_SEQ"));
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("DELETE_RULE"));
      assertEquals("product order 1_ibfk_1", rs.getString("FK_NAME"));
      // with show, meta don't know contraint name
      assertEquals(
          (i == 0 && !importedKeysWithConstraintNames) ? null : "unik_name",
          rs.getString("PK_NAME"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, rs.getInt("DEFERRABILITY"));

      assertTrue(rs.next());
      assertEquals(useCatalog ? database : "def", rs.getString("PKTABLE_CAT"));
      assertEquals(useCatalog ? null : database, rs.getString("PKTABLE_SCHEM"));
      assertEquals("cus`tomer", rs.getString("PKTABLE_NAME"));
      assertEquals("id", rs.getString("PKCOLUMN_NAME"));
      assertEquals(useCatalog ? con.getCatalog() : "def", rs.getString("FKTABLE_CAT"));
      assertEquals(useCatalog ? null : con.getSchema(), rs.getString("FKTABLE_SCHEM"));
      assertEquals("product order 1", rs.getString("FKTABLE_NAME"));
      assertEquals("customer_id", rs.getString("FKCOLUMN_NAME"));
      assertEquals(1, rs.getInt("KEY_SEQ"));
      if (isMariaDBServer() && minVersion(10, 5, 0)) {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("DELETE_RULE"));
      }
      assertEquals("product order 1_ibfk_2", rs.getString("FK_NAME"));
      // with show, meta don't know contraint name
      assertEquals(
          (i == 0 && !importedKeysWithConstraintNames) ? null : "PRIMARY", rs.getString("PK_NAME"));
      assertEquals(DatabaseMetaData.importedKeyNotDeferrable, rs.getInt("DEFERRABILITY"));
    }

    /* Also compare metadata */
    ResultSetMetaData md1 = rs1.getMetaData();
    ResultSetMetaData md2 = rs2.getMetaData();
    for (int i = 1; i <= md1.getColumnCount(); i++) {
      assertEquals(md1.getColumnLabel(i), md2.getColumnLabel(i));
    }

    rs1 = con.getMetaData().getImportedKeysUsingShowCreateTable("other sch'ema", "product order.2");
    rs2 =
        con.getMetaData().getImportedKeysUsingInformationSchema("other sch'ema", "product order.2");
    assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());
    for (int i = 0; i < 2; i++) {
      ResultSet rs = i == 0 ? rs1 : rs2;
      assertTrue(rs.next());
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }

    rs1 =
        con.getMetaData()
            .getImportedKeysUsingShowCreateTable(sharedConn.getCatalog(), "product_order.3");
    rs2 =
        con.getMetaData()
            .getImportedKeysUsingInformationSchema(sharedConn.getCatalog(), "product_order.3");
    assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());
    for (int i = 0; i < 2; i++) {
      ResultSet rs = i == 0 ? rs1 : rs2;
      assertTrue(rs.next());
      assertEquals(DatabaseMetaData.importedKeySetNull, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeySetNull, rs.getInt("DELETE_RULE"));
    }

    rs1 =
        con.getMetaData()
            .getImportedKeysUsingShowCreateTable(sharedConn.getCatalog(), "product_order4");
    rs2 =
        con.getMetaData()
            .getImportedKeysUsingInformationSchema(sharedConn.getCatalog(), "product_order4");
    assertEquals(rs1.getMetaData().getColumnCount(), rs2.getMetaData().getColumnCount());
    for (int i = 0; i < 2; i++) {
      ResultSet rs = i == 0 ? rs1 : rs2;
      if (isMariaDBServer()) {
        for (int j = 0; j < 2; j++) {
          assertTrue(rs.next());
          if ("fk1".equals(rs.getString("FK_NAME"))) {
            assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("UPDATE_RULE"));
            assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
          } else {
            assertEquals(DatabaseMetaData.importedKeySetNull, rs.getInt("UPDATE_RULE"));
            assertEquals(DatabaseMetaData.importedKeySetNull, rs.getInt("DELETE_RULE"));
          }
        }
      }
    }

    Common.assertThrowsContains(
        SQLException.class,
        () ->
            con.getMetaData()
                .getImportedKeysUsingShowCreateTable(sharedConn.getCatalog(), "UNKNO>NTABLE"),
        " doesn't exist");

    st.execute("DROP TABLE IF EXISTS `product order 1`");
    st.execute("DROP TABLE IF EXISTS `other sch'ema`.`product order.2`");
    st.execute("DROP TABLE IF EXISTS `product_order.3`");
    st.execute("DROP TABLE IF EXISTS product_order4");
    st.execute("DROP TABLE IF EXISTS t1.product ");
    st.execute("DROP TABLE IF EXISTS `cus``tomer`");
    st.execute("DROP DATABASE IF EXISTS test1");
    st.execute("DROP DATABASE IF EXISTS `other sch'ema`");
  }

  @Test
  public void exportedKeysTest() throws SQLException {

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getExportedKeys(sharedConn.getCatalog(), null, "cross%");
    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString("PKTABLE_CAT"));
    assertNull(rs.getString("PKTABLE_SCHEM"));
    assertEquals("cross1", rs.getString("PKTABLE_NAME"));
    assertEquals("id", rs.getString("PKCOLUMN_NAME"));
    assertEquals(sharedConn.getCatalog(), rs.getString("FKTABLE_CAT"));
    assertNull(rs.getString("FKTABLE_SCHEM"));
    assertEquals("cross2", rs.getString("FKTABLE_NAME"));
    assertEquals("id_ref0", rs.getString("FKCOLUMN_NAME"));
    assertTrue(rs.getInt("KEY_SEQ") == 1 || rs.wasNull());
    if (!isMariaDBServer() && minVersion(8, 0, 0)) {
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
    } else {
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }
    assertEquals("cross2_ibfk_1", rs.getString("FK_NAME"));
    assertTrue(
        "PRIMARY".equals(rs.getString("PK_NAME"))
            || "__idx_cross1__PRIMARY".equals(rs.getString("PK_NAME")));

    assertTrue(rs.next());

    assertEquals(sharedConn.getCatalog(), rs.getString("PKTABLE_CAT"));
    assertNull(rs.getString("PKTABLE_SCHEM"));
    assertEquals("cross2", rs.getString("PKTABLE_NAME"));
    assertEquals("id", rs.getString("PKCOLUMN_NAME"));
    assertEquals(sharedConn.getCatalog(), rs.getString("FKTABLE_CAT"));
    assertNull(rs.getString("FKTABLE_SCHEM"));
    assertEquals("cross3", rs.getString("FKTABLE_NAME"));
    assertEquals("id_ref1", rs.getString("FKCOLUMN_NAME"));
    assertTrue(rs.getInt("KEY_SEQ") == 1 || rs.wasNull());
    assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
    if (!isMariaDBServer() && minVersion(8, 0, 0)) {
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
    } else {
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }
    assertTrue(
        "cross3_ibfk_1".equals(rs.getString("FK_NAME"))
            || "fk_my_name".equals(rs.getString("FK_NAME")));
    assertEquals("unik_name", rs.getString("PK_NAME"));

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString("PKTABLE_CAT"));
    assertNull(rs.getString("PKTABLE_SCHEM"));
    assertEquals("cross2", rs.getString("PKTABLE_NAME"));
    assertEquals("id2", rs.getString("PKCOLUMN_NAME"));
    assertEquals(sharedConn.getCatalog(), rs.getString("FKTABLE_CAT"));
    assertNull(rs.getString("FKTABLE_SCHEM"));
    assertEquals("cross3", rs.getString("FKTABLE_NAME"));
    assertEquals("id_ref2", rs.getString("FKCOLUMN_NAME"));
    assertTrue(rs.getInt("KEY_SEQ") == 2 || rs.wasNull());
    assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
    if (!isMariaDBServer() && minVersion(8, 0, 0)) {
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
    } else {
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }
    assertTrue(
        "fk_my_name".equals(rs.getString("FK_NAME"))
            || "cross3_ibfk_1".equals(rs.getString("FK_NAME")));
    assertEquals("unik_name", rs.getString("PK_NAME"));
  }

  @Test
  public void exportedKeysTestSchema() throws SQLException {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      DatabaseMetaData dbmd = con.getMetaData();
      assertEquals(database, con.getSchema());
      assertEquals("def", con.getCatalog());
      ResultSet rs = dbmd.getExportedKeys(null, con.getSchema(), "cross%");
      assertTrue(rs.next());
      assertEquals("def", rs.getString("PKTABLE_CAT"));
      assertEquals(database, rs.getString("PKTABLE_SCHEM"));
      assertEquals("cross1", rs.getString("PKTABLE_NAME"));
      assertEquals("id", rs.getString("PKCOLUMN_NAME"));
      assertEquals("def", rs.getString("FKTABLE_CAT"));
      assertEquals(database, rs.getString("FKTABLE_SCHEM"));
      assertEquals("cross2", rs.getString("FKTABLE_NAME"));
      assertEquals("id_ref0", rs.getString("FKCOLUMN_NAME"));
      assertTrue(rs.getInt("KEY_SEQ") == 1 || rs.wasNull());
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      }
      assertEquals("cross2_ibfk_1", rs.getString("FK_NAME"));
      assertTrue(
          "PRIMARY".equals(rs.getString("PK_NAME"))
              || "__idx_cross1__PRIMARY".equals(rs.getString("PK_NAME")));

      assertTrue(rs.next());

      assertEquals("def", rs.getString("PKTABLE_CAT"));
      assertEquals(database, rs.getString("PKTABLE_SCHEM"));
      assertEquals("cross2", rs.getString("PKTABLE_NAME"));
      assertEquals("id", rs.getString("PKCOLUMN_NAME"));
      assertEquals("def", rs.getString("FKTABLE_CAT"));
      assertEquals(database, rs.getString("FKTABLE_SCHEM"));
      assertEquals("cross3", rs.getString("FKTABLE_NAME"));
      assertEquals("id_ref1", rs.getString("FKCOLUMN_NAME"));
      assertTrue(rs.getInt("KEY_SEQ") == 1 || rs.wasNull());
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      }
      assertTrue(
          "cross3_ibfk_1".equals(rs.getString("FK_NAME"))
              || "fk_my_name".equals(rs.getString("FK_NAME")));
      assertEquals("unik_name", rs.getString("PK_NAME"));

      assertTrue(rs.next());
      assertEquals(con.getCatalog(), rs.getString("PKTABLE_CAT"));
      assertEquals(database, rs.getString("PKTABLE_SCHEM"));
      assertEquals("cross2", rs.getString("PKTABLE_NAME"));
      assertEquals("id2", rs.getString("PKCOLUMN_NAME"));
      assertEquals(con.getCatalog(), rs.getString("FKTABLE_CAT"));
      assertEquals(database, rs.getString("FKTABLE_SCHEM"));
      assertEquals("cross3", rs.getString("FKTABLE_NAME"));
      assertEquals("id_ref2", rs.getString("FKCOLUMN_NAME"));
      assertTrue(rs.getInt("KEY_SEQ") == 2 || rs.wasNull());
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt("UPDATE_RULE"));
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      }
      assertTrue(
          "fk_my_name".equals(rs.getString("FK_NAME"))
              || "cross3_ibfk_1".equals(rs.getString("FK_NAME")));
      assertEquals("unik_name", rs.getString("PK_NAME"));
    }
  }

  @Test
  public void importedKeysTest() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("drop table if exists fore_key0");
    stmt.execute("drop table if exists fore_key1");
    stmt.execute("drop table if exists prim_key");

    stmt.execute(
        "create table prim_key (id int not null primary key, " + "val varchar(20)) engine=innodb");
    stmt.execute(
        "create table fore_key0 (id int not null primary key, "
            + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
    stmt.execute(
        "create table fore_key1 (id int not null primary key, id_ref1 int, foreign key (id_ref1)"
            + " references prim_key(id) on update cascade) engine=innodb");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getImportedKeys(sharedConn.getCatalog(), null, "fore_key0");
    int counter = 0;
    while (rs.next()) {
      assertEquals("id", rs.getString("pkcolumn_name"));
      assertEquals("prim_key", rs.getString("pktable_name"));
      counter++;
    }
    assertEquals(1, counter);
    stmt.execute("drop table if exists fore_key0");
    stmt.execute("drop table if exists fore_key1");
    stmt.execute("drop table if exists prim_key");
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();

    ResultSet rs = dbmd.getCatalogs();
    boolean haveMysql = false;
    boolean haveInformationSchema = false;
    while (rs.next()) {
      String cat = rs.getString(1);

      if (cat.equalsIgnoreCase("mysql")) {
        haveMysql = true;
      } else if (cat.equalsIgnoreCase("information_schema")) {
        haveInformationSchema = true;
      }
    }
    if (!isXpand()) assertTrue(haveMysql);
    assertTrue(haveInformationSchema);
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      dbmd = con.getMetaData();
      rs = dbmd.getCatalogs();
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetTables() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("drop table if exists fore_key0");
    stmt.execute("drop table if exists fore_key1");
    stmt.execute("drop table if exists prim_key");

    stmt.execute(
        "create table prim_key (id int not null primary key, " + "val varchar(20)) engine=innodb");
    stmt.execute(
        "create table fore_key0 (id int not null primary key, "
            + "id_ref0 int, foreign key (id_ref0) references prim_key(id)) engine=innodb");
    stmt.execute(
        "create table fore_key1 (id int not null primary key, id_ref1 int, foreign key (id_ref1)"
            + " references prim_key(id) on update cascade) engine=innodb");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getTables(null, null, "prim_key", null);

    assertTrue(rs.next());
    rs = dbmd.getTables("", null, "prim_key", null);
    assertTrue(rs.next());

    rs = dbmd.getTables("", null, "prim_key", new String[] {"BASE TABLE", "OTHER"});
    assertTrue(rs.next());

    rs = dbmd.getTables("", null, "prim_key", new String[] {"TABLE", null});
    assertTrue(rs.next());

    // ensure no regression CONJ-921
    rs = dbmd.getTables(null, null, null, new String[] {"TABLE"});
    assertTrue(rs.next());
  }

  @Test
  public void testGetTables2() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    Assumptions.assumeTrue(!isXpand());
    ResultSet rs =
        dbmd.getTables(
            "information_schema", null, "TABLE_PRIVILEGES", new String[] {"SYSTEM VIEW"});
    assertTrue(rs.next());
    assertFalse(rs.next());
    rs = dbmd.getTables(null, null, "TABLE_PRIVILEGES", new String[] {"TABLE"});
    assertFalse(rs.next());
  }

  @Test
  public void testGetTablesSystemVersionTables() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 3, 4));
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getTables(null, null, "versionTable", null);
    assertTrue(rs.next());
    assertFalse(rs.next());
    rs = dbmd.getTables(null, null, "versionTable", new String[] {"TABLE"});
    assertTrue(rs.next());
    assertFalse(rs.next());
    rs = dbmd.getTables(null, null, "versionTable", new String[] {"SYSTEM VIEW"});
    assertFalse(rs.next());
  }

  @Test
  public void testGetTables3() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("drop table if exists table_type_test");

    stmt.execute(
        "create table table_type_test (id int not null primary key, "
            + "val varchar(20)) engine=innodb");

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet tableSet = dbmd.getTables(null, null, "table_type_test", null);

    assertTrue(tableSet.next());

    String tableName = tableSet.getString("TABLE_NAME");
    assertEquals("table_type_test", tableName);

    String tableType = tableSet.getString("TABLE_TYPE");
    assertEquals("TABLE", tableType);
    // see for possible values
    // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTableTypes%28%29
  }

  @Test
  public void testGetColumns() throws SQLException {
    // mysql 5.6 doesn't permit VIRTUAL keyword
    Assumptions.assumeTrue(isMariaDBServer() || !isMariaDBServer() && minVersion(5, 7, 0));
    // Xpand doesn't support PERSISTENT keyword
    Assumptions.assumeFalse(isXpand());

    Statement stmt = sharedConn.createStatement();
    if (minVersion(10, 2, 0) || !isMariaDBServer()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `ta\n"
              + "ble'getcolumns`(a INT NOT NULL primary key auto_increment, b VARCHAR(32), c INT AS"
              + " (CHAR_LENGTH(b)) VIRTUAL, d VARCHAR(5) AS (left(b,5)) STORED) CHARACTER SET"
              + " 'utf8mb4'");
    } else {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `ta\n"
              + "ble'getcolumns`(a INT NOT NULL primary key auto_increment, b VARCHAR(32), c INT AS"
              + " (CHAR_LENGTH(b)) VIRTUAL, d VARCHAR(5) AS (left(b,5)) PERSISTENT) CHARACTER SET"
              + " 'utf8mb4'");
    }

    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getColumns(null, null, "ta\nble'getcolumns", null);

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertNull(rs.getString(2)); // TABLE_SCHEM
    assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("a", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
    assertEquals("INT", rs.getString(6)); // "TYPE_NAME
    assertEquals(10, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(0, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS
    assertNull(rs.getString(13)); // COLUMN_DEF
    assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(1, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("NO", rs.getString(18)); // IS_NULLABLE
    assertNull(rs.getString(19)); // SCOPE_CATALOG
    assertNull(rs.getString(20)); // SCOPE_SCHEMA
    assertNull(rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("YES", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertNull(rs.getString(2)); // TABLE_SCHEM
    assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("b", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
    assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
    assertEquals(32, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS

    // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
    // require
    assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
    assertEquals(32 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(2, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("YES", rs.getString(18)); // IS_NULLABLE
    assertNull(rs.getString(19)); // SCOPE_CATALOG
    assertNull(rs.getString(20)); // SCOPE_SCHEMA
    assertNull(rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertNull(rs.getString(2)); // TABLE_SCHEM
    assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("c", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
    assertEquals("INT", rs.getString(6)); // "TYPE_NAME
    assertEquals(10, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS

    // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
    // require
    assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF

    assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(3, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("YES", rs.getString(18)); // IS_NULLABLE
    assertNull(rs.getString(19)); // SCOPE_CATALOG
    assertNull(rs.getString(20)); // SCOPE_SCHEMA
    assertNull(rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1)); // TABLE_CAT
    assertNull(rs.getString(2)); // TABLE_SCHEM
    assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
    assertEquals("d", rs.getString(4)); // COLUMN_NAME
    assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
    assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
    assertEquals(5, rs.getInt(7)); // "COLUMN_SIZE
    assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
    assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
    assertEquals(1, rs.getInt(11)); // NULLABLE
    assertEquals("", rs.getString(12)); // REMARKS
    // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
    // require
    assertTrue("null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
    assertEquals(5 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
    assertEquals(4, rs.getInt(17)); // ORDINAL_POSITION
    assertEquals("YES", rs.getString(18)); // IS_NULLABLE
    assertNull(rs.getString(19)); // SCOPE_CATALOG
    assertNull(rs.getString(20)); // SCOPE_SCHEMA
    assertNull(rs.getString(21)); // SCOPE_TABLE
    assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
    assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
    assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN
    assertFalse(rs.next());
  }

  @Test
  public void testGetColumnsSchema() throws SQLException {
    // mysql 5.6 doesn't permit VIRTUAL keyword
    Assumptions.assumeTrue(isMariaDBServer() || !isMariaDBServer() && minVersion(5, 7, 0));
    // Xpand doesn't support PERSISTENT keyword
    Assumptions.assumeFalse(isXpand());
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      System.out.println(con.getSchema());
      java.sql.Statement stmt = con.createStatement();
      if (minVersion(10, 2, 0) || !isMariaDBServer()) {
        stmt.execute(
            "CREATE TABLE IF NOT EXISTS "
                + database
                + ".`ta\n"
                + "ble'getcolumns`(a INT NOT NULL primary key auto_increment, b VARCHAR(32), c INT"
                + " AS (CHAR_LENGTH(b)) VIRTUAL, d VARCHAR(5) AS (left(b,5)) STORED) CHARACTER SET"
                + " 'utf8mb4'");
      } else {
        stmt.execute(
            "CREATE TABLE IF NOT EXISTS \"+database+\".`ta\n"
                + "ble'getcolumns`(a INT NOT NULL primary key auto_increment, b VARCHAR(32), c INT"
                + " AS (CHAR_LENGTH(b)) VIRTUAL, d VARCHAR(5) AS (left(b,5)) PERSISTENT) CHARACTER"
                + " SET 'utf8mb4'");
      }

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getColumns(null, null, "ta\nble'getcolumns", null);

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1)); // TABLE_CAT
      assertEquals(database, rs.getString(2)); // TABLE_SCHEM
      assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
      assertEquals("a", rs.getString(4)); // COLUMN_NAME
      assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
      assertEquals("INT", rs.getString(6)); // "TYPE_NAME
      assertEquals(10, rs.getInt(7)); // "COLUMN_SIZE
      assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
      assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
      assertEquals(0, rs.getInt(11)); // NULLABLE
      assertEquals("", rs.getString(12)); // REMARKS
      assertNull(rs.getString(13)); // COLUMN_DEF
      assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
      assertEquals(1, rs.getInt(17)); // ORDINAL_POSITION
      assertEquals("NO", rs.getString(18)); // IS_NULLABLE
      assertNull(rs.getString(19)); // SCOPE_CATALOG
      assertNull(rs.getString(20)); // SCOPE_SCHEMA
      assertNull(rs.getString(21)); // SCOPE_TABLE
      assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
      assertEquals("YES", rs.getString(23)); // IS_AUTOINCREMENT
      assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1)); // TABLE_CAT
      assertEquals(database, rs.getString(2)); // TABLE_SCHEM
      assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
      assertEquals("b", rs.getString(4)); // COLUMN_NAME
      assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
      assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
      assertEquals(32, rs.getInt(7)); // "COLUMN_SIZE
      assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
      assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
      assertEquals(1, rs.getInt(11)); // NULLABLE
      assertEquals("", rs.getString(12)); // REMARKS

      // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
      // require
      assertTrue(
          "null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
      assertEquals(32 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
      assertEquals(2, rs.getInt(17)); // ORDINAL_POSITION
      assertEquals("YES", rs.getString(18)); // IS_NULLABLE
      assertNull(rs.getString(19)); // SCOPE_CATALOG
      assertNull(rs.getString(20)); // SCOPE_SCHEMA
      assertNull(rs.getString(21)); // SCOPE_TABLE
      assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
      assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
      assertEquals("NO", rs.getString(24)); // IS_GENERATEDCOLUMN

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1)); // TABLE_CAT
      assertEquals(database, rs.getString(2)); // TABLE_SCHEM
      assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
      assertEquals("c", rs.getString(4)); // COLUMN_NAME
      assertEquals(Types.INTEGER, rs.getInt(5)); // DATA_TYPE
      assertEquals("INT", rs.getString(6)); // "TYPE_NAME
      assertEquals(10, rs.getInt(7)); // "COLUMN_SIZE
      assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
      assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
      assertEquals(1, rs.getInt(11)); // NULLABLE
      assertEquals("", rs.getString(12)); // REMARKS

      // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
      // require
      assertTrue(
          "null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF

      assertEquals(0, rs.getInt(16)); // CHAR_OCTET_LENGTH
      assertEquals(3, rs.getInt(17)); // ORDINAL_POSITION
      assertEquals("YES", rs.getString(18)); // IS_NULLABLE
      assertNull(rs.getString(19)); // SCOPE_CATALOG
      assertNull(rs.getString(20)); // SCOPE_SCHEMA
      assertNull(rs.getString(21)); // SCOPE_TABLE
      assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
      assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
      assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1)); // TABLE_CAT
      assertEquals(database, rs.getString(2)); // TABLE_SCHEM
      assertEquals("ta\nble'getcolumns", rs.getString(3)); // TABLE_NAME
      assertEquals("d", rs.getString(4)); // COLUMN_NAME
      assertEquals(Types.VARCHAR, rs.getInt(5)); // DATA_TYPE
      assertEquals("VARCHAR", rs.getString(6)); // "TYPE_NAME
      assertEquals(5, rs.getInt(7)); // "COLUMN_SIZE
      assertEquals(0, rs.getInt(9)); // DECIMAL_DIGITS
      assertEquals(10, rs.getInt(10)); // NUM_PREC_RADIX
      assertEquals(1, rs.getInt(11)); // NULLABLE
      assertEquals("", rs.getString(12)); // REMARKS
      // since 10.2.7, value that are expected as String are enclosed with single quotes as javadoc
      // require
      assertTrue(
          "null".equalsIgnoreCase(rs.getString(13)) || rs.getString(13) == null); // COLUMN_DEF
      assertEquals(5 * 4, rs.getInt(16)); // CHAR_OCTET_LENGTH
      assertEquals(4, rs.getInt(17)); // ORDINAL_POSITION
      assertEquals("YES", rs.getString(18)); // IS_NULLABLE
      assertNull(rs.getString(19)); // SCOPE_CATALOG
      assertNull(rs.getString(20)); // SCOPE_SCHEMA
      assertNull(rs.getString(21)); // SCOPE_TABLE
      assertEquals(0, rs.getShort(22)); // SOURCE_DATA_TYPE
      assertEquals("NO", rs.getString(23)); // IS_AUTOINCREMENT
      assertEquals("YES", rs.getString(24)); // IS_GENERATEDCOLUMN
      assertFalse(rs.next());
    }
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

  private void testGetColumnstinyInt1isBit(Connection con) throws SQLException {
    try {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `tinyInt1\nisBitCols`(id1 tinyint(1), id2 tinyint(2))");
      stmt.execute("INSERT INTO `tinyInt1\nisBitCols` VALUES (1,2)");

      ResultSet rs1 =
          sharedConn.createStatement().executeQuery("SELECT * FROM `tinyInt1\nisBitCols`");
      assertTrue(rs1.next());
      assertEquals(Boolean.TRUE, rs1.getObject(1));
      assertEquals(2, rs1.getObject(2));

      rs1 = stmt.executeQuery("SELECT * FROM `tinyInt1\nisBitCols`");
      assertTrue(rs1.next());
      assertEquals(1, rs1.getObject(1));
      assertEquals(2, rs1.getObject(2));

      DatabaseMetaData dbmd = sharedConn.getMetaData();
      ResultSet rs = dbmd.getColumns(null, null, "tinyInt1\nisBitCols", null);

      assertTrue(rs.next());
      assertEquals(Types.BOOLEAN, rs.getInt(5));
      assertTrue(rs.next());
      assertEquals(Types.TINYINT, rs.getInt(5));

      dbmd = con.getMetaData();
      rs = dbmd.getColumns(null, null, "tinyInt1\nisBitCols", null);

      assertTrue(rs.next());
      assertEquals(Types.TINYINT, rs.getInt(5));
      assertTrue(rs.next());
      assertEquals(Types.TINYINT, rs.getInt(5));

    } finally {
      con.createStatement().execute("DROP TABLE IF EXISTS `tinyInt1\nisBitCols`");
      con.rollback();
    }
  }

  @Test
  public void testTransformedBitIsBoolean() throws SQLException {
    try (Connection con = createCon("tinyInt1isBit=true&transformedBitIsBoolean=true")) {
      testTransformedBitIsBoolean(con, true, true);
    }
    try (Connection con = createCon("tinyInt1isBit=true&transformedBitIsBoolean=false")) {
      testTransformedBitIsBoolean(con, true, false);
    }
    try (Connection con = createCon("tinyInt1isBit=false&transformedBitIsBoolean=true")) {
      testTransformedBitIsBoolean(con, false, true);
    }
    try (Connection con = createCon("transformedBitIsBoolean=true")) {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("SET sql_mode = concat(@@sql_mode,',NO_BACKSLASH_ESCAPES')");
      testTransformedBitIsBoolean(con, true, true);
    }
  }

  private void testTransformedBitIsBoolean(
      Connection con, boolean tinyInt1isBit, boolean transformedBitIsBoolean) throws SQLException {
    try {
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS `tinyInt1\nisBitCols`(id1 tinyint(1), id2 tinyint(2))");
      stmt.execute("INSERT INTO `tinyInt1\nisBitCols` VALUES (1,2)");

      ResultSet rs1 = con.createStatement().executeQuery("SELECT * FROM `tinyInt1\nisBitCols`");
      assertTrue(rs1.next());
      if (tinyInt1isBit) {
        assertEquals(Boolean.TRUE, rs1.getObject(1));
        assertEquals(2, rs1.getObject(2));
      } else {
        assertEquals(1, rs1.getObject(1));
        assertEquals(2, rs1.getObject(2));
      }

      ResultSetMetaData rsm = rs1.getMetaData();
      if (tinyInt1isBit) {
        if (transformedBitIsBoolean) {
          assertEquals(Types.BOOLEAN, rsm.getColumnType(1));
          assertEquals("BOOLEAN", rsm.getColumnTypeName(1));
        } else {
          assertEquals(Types.BIT, rsm.getColumnType(1));
          assertEquals("BIT", rsm.getColumnTypeName(1));
        }
      } else {
        assertEquals(Types.TINYINT, rsm.getColumnType(1));
        assertEquals("TINYINT", rsm.getColumnTypeName(1));
      }

      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getColumns(null, null, "tinyInt1\nisBitCols", null);

      assertTrue(rs.next());
      if (tinyInt1isBit) {
        if (transformedBitIsBoolean) {
          assertEquals(Types.BOOLEAN, rs.getInt(5));
          assertEquals("BOOLEAN", rs.getString(6));
        } else {
          assertEquals(Types.BIT, rs.getInt(5));
          assertEquals("BIT", rs.getString(6));
        }
      } else {
        assertEquals(Types.TINYINT, rs.getInt(5));
        assertEquals("TINYINT", rs.getString(6));
      }
      assertTrue(rs.next());
      assertEquals(Types.TINYINT, rs.getInt(5));

    } finally {
      con.createStatement().execute("DROP TABLE IF EXISTS `tinyInt1\nisBitCols`");
      con.rollback();
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
              columnType == Types.VARCHAR
                  || columnType == Types.CHAR
                  || columnType == Types.NULL
                  || columnType == Types.LONGVARCHAR,
              "invalid type "
                  + columnType
                  + " for "
                  + rsmd.getColumnLabel(col)
                  + ",expected String");
          break;
        case "decimal":
          assertEquals(
              Types.DECIMAL,
              columnType,
              "invalid type  "
                  + columnType
                  + "( "
                  + rsmd.getColumnTypeName(col)
                  + " ) for "
                  + rsmd.getColumnLabel(col)
                  + ",expected decimal");
          break;
        case "int":
        case "short":
          assertTrue(
              columnType == Types.BIGINT
                  || columnType == Types.INTEGER
                  || columnType == Types.SMALLINT
                  || columnType == Types.TINYINT,
              "invalid type  "
                  + columnType
                  + "( "
                  + rsmd.getColumnTypeName(col)
                  + " ) for "
                  + rsmd.getColumnLabel(col)
                  + ",expected numeric");

          break;
        case "boolean":
          assertTrue(
              columnType == Types.BOOLEAN || columnType == Types.BIT,
              "invalid type  "
                  + columnType
                  + "( "
                  + rsmd.getColumnTypeName(col)
                  + " ) for "
                  + rsmd.getColumnLabel(col)
                  + ",expected boolean");

          break;
        case "null":
          assertEquals(
              Types.NULL,
              columnType,
              "invalid type  "
                  + columnType
                  + " for "
                  + rsmd.getColumnLabel(col)
                  + ",expected null");
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

    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      dbmd = con.getMetaData();
      rs = dbmd.getSchemas();
      assertTrue(rs.next());
      rs = dbmd.getSchemas("", "%");
      assertTrue(rs.next());
    }
  }

  @Test
  public void getAttributesBasic() throws Exception {
    testResultSetColumns(
        sharedConn.getMetaData().getAttributes(null, null, null, null),
        "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,ATTR_NAME String,DATA_TYPE"
            + " int,ATTR_TYPE_NAME String,ATTR_SIZE int,DECIMAL_DIGITS int,NUM_PREC_RADIX"
            + " int,NULLABLE int,REMARKS String,ATTR_DEF String,SQL_DATA_TYPE int,SQL_DATETIME_SUB"
            + " int, CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE String,SCOPE_CATALOG"
            + " String,SCOPE_SCHEMA String,SCOPE_TABLE String,SOURCE_DATA_TYPE short");
  }

  @Test
  public void identifierCaseSensitivity() throws Exception {
    // https://jira.mariadb.org/browse/XPT-281
    Assumptions.assumeFalse(isXpand());
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
    Assumptions.assumeFalse(isXpand());
    DatabaseMetaData meta = sharedConn.getMetaData();
    Common.assertThrowsContains(
        SQLException.class,
        () -> meta.getBestRowIdentifier(null, null, null, 0, true),
        "'table' parameter cannot be null in getBestRowIdentifier()");
    testResultSetColumns(
        meta.getBestRowIdentifier(null, null, "", 0, true),
        "SCOPE short,COLUMN_NAME String,DATA_TYPE int, TYPE_NAME String,"
            + "COLUMN_SIZE int,BUFFER_LENGTH int,"
            + "DECIMAL_DIGITS short,PSEUDO_COLUMN short");

    ResultSet rs = meta.getBestRowIdentifier(null, null, "cross1", 0, true);
    assertTrue(rs.next());

    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());

    rs = meta.getBestRowIdentifier(null, null, "cross1", 0, false);
    assertTrue(rs.next());
    assertFalse(rs.next());

    rs = meta.getBestRowIdentifier(null, null, "cross2", 0, true);
    assertTrue(rs.next());
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertTrue(rs.next());
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id2", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());

    rs = meta.getBestRowIdentifier(null, null, "cross3", 0, true);
    assertTrue(rs.next());

    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());

    // CHECK using PRI even if exist UNI

    rs = meta.getBestRowIdentifier(null, null, "getBestRowIdentifier1", 0, true);
    assertTrue(rs.next());

    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("i", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());

    rs = meta.getBestRowIdentifier(null, null, "getBestRowIdentifier2", 0, true);
    assertTrue(rs.next());
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id_ref0", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertTrue(rs.next());

    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowSession, rs.getInt(1));
    assertEquals("id_ref2", rs.getString(2));
    assertEquals(Types.INTEGER, rs.getInt(3));
    assertEquals("int", rs.getString(4));
    assertEquals(10, rs.getInt(5));
    assertEquals(0, rs.getInt(6));
    assertEquals(0, rs.getInt(7));
    assertEquals(org.mariadb.jdbc.DatabaseMetaData.bestRowNotPseudo, rs.getInt(8));
    assertFalse(rs.next());
  }

  @Test
  public void getClientInfoPropertiesBasic() throws Exception {
    testResultSetColumns(
        sharedConn.getMetaData().getClientInfoProperties(),
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
        "The name of the user that the application using the connection is performing work for."
            + " This may not be the same as the user name that was used in establishing the"
            + " connection.",
        rs.getString(4));

    assertTrue(rs.next());
    assertEquals("ClientHostname", rs.getString(1));
    assertEquals(0x00ffffff, rs.getInt(2));
    assertEquals("", rs.getString(3));
    assertEquals(
        "The hostname of the computer the application using the connection is running on",
        rs.getString(4));

    assertFalse(rs.next());
  }

  @Test
  public void getCatalogsBasic() throws SQLException {
    testResultSetColumns(sharedConn.getMetaData().getCatalogs(), "TABLE_CAT String");
  }

  @Test
  public void getColumnsBasic() throws SQLException {
    Assumptions.assumeFalse(isXpand());
    cancelForVersion(10, 1); // due to server error MDEV-8984
    if (minVersion(10, 2, 0)) {
      testResultSetColumns(
          sharedConn.getMetaData().getColumns(null, null, null, null),
          "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
              + "DATA_TYPE int,TYPE_NAME String,COLUMN_SIZE decimal,BUFFER_LENGTH int,"
              + "DECIMAL_DIGITS int,NUM_PREC_RADIX int,NULLABLE int,"
              + "REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
              + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH decimal,"
              + "ORDINAL_POSITION int,IS_NULLABLE String,"
              + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
              + "SCOPE_TABLE String,SOURCE_DATA_TYPE null");
    } else {
      testResultSetColumns(
          sharedConn.getMetaData().getColumns(null, null, null, null),
          "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
              + "DATA_TYPE int,TYPE_NAME String,COLUMN_SIZE int,BUFFER_LENGTH int,"
              + "DECIMAL_DIGITS int,NUM_PREC_RADIX int,NULLABLE int,"
              + "REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE int,"
              + "SQL_DATETIME_SUB int, CHAR_OCTET_LENGTH int,"
              + "ORDINAL_POSITION int,IS_NULLABLE String,"
              + "SCOPE_CATALOG String,SCOPE_SCHEMA String,"
              + "SCOPE_TABLE String,SOURCE_DATA_TYPE null");
    }
  }

  @Test
  public void getProcedureColumnsBasic() throws SQLException {
    Assumptions.assumeTrue(!isXpand());
    testResultSetColumns(
        sharedConn.getMetaData().getProcedureColumns(null, null, null, null),
        "PROCEDURE_CAT String,PROCEDURE_SCHEM String,PROCEDURE_NAME String,COLUMN_NAME String"
            + " ,COLUMN_TYPE short,DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH int,SCALE"
            + " short,RADIX short,NULLABLE short,REMARKS String,COLUMN_DEF String,SQL_DATA_TYPE"
            + " int,SQL_DATETIME_SUB int ,CHAR_OCTET_LENGTH int,ORDINAL_POSITION int,IS_NULLABLE"
            + " String,SPECIFIC_NAME String");
  }

  @Test
  public void getFunctionColumnsBasic() throws SQLException {
    // https://jira.mariadb.org/browse/XPT-267
    Assumptions.assumeFalse(isXpand());
    testResultSetColumns(
        sharedConn.getMetaData().getFunctionColumns(null, null, null, null),
        "FUNCTION_CAT String,FUNCTION_SCHEM String,FUNCTION_NAME String,COLUMN_NAME"
            + " String,COLUMN_TYPE short,DATA_TYPE int,TYPE_NAME String,PRECISION int,LENGTH"
            + " int,SCALE short,RADIX short,NULLABLE short,REMARKS String,CHAR_OCTET_LENGTH"
            + " int,ORDINAL_POSITION int,IS_NULLABLE String,SPECIFIC_NAME String");
  }

  @Test
  public void getColumnPrivilegesBasic() throws SQLException {
    Assumptions.assumeFalse(isXpand());
    Common.assertThrowsContains(
        SQLException.class,
        () -> sharedConn.getMetaData().getColumnPrivileges(null, null, null, null),
        "'table' parameter must not be null");
    testResultSetColumns(
        sharedConn.getMetaData().getColumnPrivileges(null, null, "", null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,"
            + "GRANTOR String,GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
  }

  @Test
  public void getTablePrivilegesBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getTablePrivileges(null, null, null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,GRANTOR String,"
            + "GRANTEE String,PRIVILEGE String,IS_GRANTABLE String");
  }

  @Test
  public void getVersionColumnsBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getVersionColumns(null, null, null),
        "SCOPE short, COLUMN_NAME String,DATA_TYPE int,TYPE_NAME String,"
            + "COLUMN_SIZE int,BUFFER_LENGTH int,DECIMAL_DIGITS short,"
            + "PSEUDO_COLUMN short");
  }

  @Test
  public void getPrimaryKeysBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getPrimaryKeys(null, null, null),
        "TABLE_CAT String,TABLE_SCHEM String,TABLE_NAME String,COLUMN_NAME String,KEY_SEQ"
            + " short,PK_NAME String");
  }

  @Test
  public void getImportedKeysBasic() throws SQLException {
    Assumptions.assumeFalse(isXpand());
    testResultSetColumns(
        sharedConn.getMetaData().getImportedKeys(null, null, ""),
        "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME"
            + " String,FKTABLE_CAT String,FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME"
            + " String,KEY_SEQ short,UPDATE_RULE short,DELETE_RULE short,FK_NAME String,PK_NAME"
            + " String,DEFERRABILITY short");
  }

  @Test
  public void getExportedKeysBasic() throws SQLException {
    Assumptions.assumeFalse(isXpand());
    testResultSetColumns(
        sharedConn.getMetaData().getExportedKeys(null, null, ""),
        "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME"
            + " String,FKTABLE_CAT String,FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME"
            + " String,KEY_SEQ short,UPDATE_RULE short, DELETE_RULE short,FK_NAME String,PK_NAME"
            + " String,DEFERRABILITY short");
  }

  @Test
  public void getCrossReferenceBasic() throws SQLException {
    Assumptions.assumeFalse(isXpand());
    testResultSetColumns(
        sharedConn.getMetaData().getCrossReference(null, null, "", null, null, ""),
        "PKTABLE_CAT String,PKTABLE_SCHEM String,PKTABLE_NAME String, PKCOLUMN_NAME"
            + " String,FKTABLE_CAT String,FKTABLE_SCHEM String,FKTABLE_NAME String,FKCOLUMN_NAME"
            + " String,KEY_SEQ short,UPDATE_RULE short,DELETE_RULE short,FK_NAME String,PK_NAME"
            + " String,DEFERRABILITY short");
  }

  @Test
  public void getCrossReferenceResults() throws SQLException {
    DatabaseMetaData dbmd = sharedConn.getMetaData();
    ResultSet rs = dbmd.getCrossReference(null, null, "cross%", null, null, "cross%");

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("cross1", rs.getString(3));
    assertEquals("id", rs.getString(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertNull(rs.getString(6));
    assertEquals("cross2", rs.getString(7));
    assertEquals("id_ref0", rs.getString(8));
    assertTrue(rs.getInt(9) == 1 || rs.wasNull());
    if (!isMariaDBServer() && minVersion(8, 0, 0)) {
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
    } else {
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }
    assertEquals("cross2_ibfk_1", rs.getString(12));

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("cross2", rs.getString(3));
    assertEquals("id", rs.getString(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertNull(rs.getString(6));
    assertEquals("cross3", rs.getString(7));
    assertEquals("id_ref1", rs.getString(8));
    assertTrue(rs.getInt(9) == 1 || rs.wasNull());
    assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt(10));
    if (!isMariaDBServer() && minVersion(8, 0, 0)) {
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
    } else {
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }
    assertTrue(
        "fk_my_name".equals(rs.getString("FK_NAME"))
            || "cross3_ibfk_1".equals(rs.getString("FK_NAME")));

    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("cross2", rs.getString(3));
    assertEquals("id2", rs.getString(4));
    assertEquals(sharedConn.getCatalog(), rs.getString(5));
    assertNull(rs.getString(6));
    assertEquals("cross3", rs.getString(7));
    assertEquals("id_ref2", rs.getString(8));
    assertTrue(rs.getInt(9) == 2 || rs.wasNull());
    assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt(10));
    if (!isMariaDBServer() && minVersion(8, 0, 0)) {
      assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
    } else {
      assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
    }
    assertTrue("fk_my_name".equals(rs.getString(12)) || "cross3_ibfk_1".equals(rs.getString(12)));

    assertFalse(rs.next());
  }

  @Test
  public void getCrossReferenceResultsSchema() throws SQLException {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getCrossReference(null, null, "cross%", null, null, "cross%");

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("cross1", rs.getString(3));
      assertEquals("id", rs.getString(4));
      assertEquals("def", rs.getString(5));
      assertEquals(database, rs.getString(6));
      assertEquals("cross2", rs.getString(7));
      assertEquals("id_ref0", rs.getString(8));
      assertTrue(rs.getInt(9) == 1 || rs.wasNull());
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      }
      assertEquals("cross2_ibfk_1", rs.getString(12));

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("cross2", rs.getString(3));
      assertEquals("id", rs.getString(4));
      assertEquals("def", rs.getString(5));
      assertEquals(database, rs.getString(6));
      assertEquals("cross3", rs.getString(7));
      assertEquals("id_ref1", rs.getString(8));
      assertTrue(rs.getInt(9) == 1 || rs.wasNull());
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt(10));
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      }
      assertTrue(
          "fk_my_name".equals(rs.getString("FK_NAME"))
              || "cross3_ibfk_1".equals(rs.getString("FK_NAME")));

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("cross2", rs.getString(3));
      assertEquals("id2", rs.getString(4));
      assertEquals("def", rs.getString(5));
      assertEquals(database, rs.getString(6));
      assertEquals("cross3", rs.getString(7));
      assertEquals("id_ref2", rs.getString(8));
      assertTrue(rs.getInt(9) == 2 || rs.wasNull());
      assertEquals(DatabaseMetaData.importedKeyCascade, rs.getInt(10));
      if (!isMariaDBServer() && minVersion(8, 0, 0)) {
        assertEquals(DatabaseMetaData.importedKeyNoAction, rs.getInt("DELETE_RULE"));
      } else {
        assertEquals(DatabaseMetaData.importedKeyRestrict, rs.getInt("DELETE_RULE"));
      }
      assertTrue("fk_my_name".equals(rs.getString(12)) || "cross3_ibfk_1".equals(rs.getString(12)));

      assertFalse(rs.next());
    }
  }

  @Test
  public void getUdtsBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getUDTs(null, null, null, null),
        "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,CLASS_NAME String,DATA_TYPE int,"
            + "REMARKS String,BASE_TYPE short");
  }

  @Test
  public void getSuperTypesBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getSuperTypes(null, null, null),
        "TYPE_CAT String,TYPE_SCHEM String,TYPE_NAME String,SUPERTYPE_CAT String,"
            + "SUPERTYPE_SCHEM String,SUPERTYPE_NAME String");
  }

  @Test
  public void getFunctionsBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getFunctions(null, null, null),
        "FUNCTION_CAT String, FUNCTION_SCHEM String,FUNCTION_NAME String,REMARKS"
            + " String,FUNCTION_TYPE short, SPECIFIC_NAME String");
  }

  @Test
  public void getSuperTablesBasic() throws SQLException {
    testResultSetColumns(
        sharedConn.getMetaData().getSuperTables(null, null, null),
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
    assertTrue(foundTestUnitsJdbc);
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      dbmd = con.getMetaData();
      rs = dbmd.getCatalogs();
      assertFalse(rs.next());
    }
  }

  @Test
  public void testGetTypeInfoBasic() throws SQLException {
    ResultSet rs = sharedConn.getMetaData().getTypeInfo();
    while (rs.next()) {
      rs.getInt(2);
    }
    ResultSetMetaData rmeta = rs.getMetaData();
    assertTrue(rmeta.isSigned(1));

    testResultSetColumns(
        rs,
        "TYPE_NAME String,DATA_TYPE int,PRECISION int,LITERAL_PREFIX String,LITERAL_SUFFIX"
            + " String,CREATE_PARAMS String, NULLABLE short,CASE_SENSITIVE boolean,SEARCHABLE"
            + " short,UNSIGNED_ATTRIBUTE boolean,FIXED_PREC_SCALE boolean, AUTO_INCREMENT boolean,"
            + " LOCAL_TYPE_NAME String,MINIMUM_SCALE short,MAXIMUM_SCALE short,SQL_DATA_TYPE"
            + " int,SQL_DATETIME_SUB int, NUM_PREC_RADIX int");
  }

  @Test
  public void getColumnsTest() throws SQLException {
    // https://jira.mariadb.org/browse/XPT-280
    Assumptions.assumeTrue(!isXpand());

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getColumns(sharedConn.getCatalog(), null, "manycols", null);
    while (rs.next()) {
      String columnName = rs.getString("column_name");
      int type = rs.getInt("data_type");
      String typeName = rs.getString("type_name");
      assertFalse(typeName.contains("("));
      for (char c : typeName.toCharArray()) {
        assertTrue(c == ' ' || Character.isUpperCase(c), "bad typename " + typeName);
      }
      checkType(columnName, type, "tiny", Types.TINYINT);
      checkType(columnName, type, "tiny_uns", Types.TINYINT);
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
    }
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
  public void getPrecision() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS getPrecision("
            + "num1 NUMERIC(9,4), "
            + "num2 NUMERIC (9,0),"
            + "num3 NUMERIC (9,4) UNSIGNED,"
            + "num4 NUMERIC (9,0) UNSIGNED,"
            + "num5 FLOAT(9,4),"
            + "num6 FLOAT(9,4) UNSIGNED,"
            + "num7 DOUBLE(9,4),"
            + "num8 DOUBLE(9,4) UNSIGNED)");
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
    assertEquals(9, rsmd.getPrecision(5));
    assertEquals(4, rsmd.getScale(5));
    assertEquals(9, rsmd.getPrecision(6));
    assertEquals(4, rsmd.getScale(6));
    assertEquals(9, rsmd.getPrecision(7));
    assertEquals(4, rsmd.getScale(7));
    assertEquals(9, rsmd.getPrecision(8));
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
    if (!isXpand()) assertEquals(6, rsmd.getScale(3));
    // timestamp(0)
    assertEquals(19, rsmd.getPrecision(4));
    assertEquals(0, rsmd.getScale(4));
    // timestamp(6)
    assertEquals(26, rsmd.getPrecision(5));
    if (!isXpand()) assertEquals(6, rsmd.getScale(5));
    // time(0)
    assertEquals(10, rsmd.getPrecision(6));
    assertEquals(0, rsmd.getScale(6));
    // time(6)
    assertEquals(17, rsmd.getPrecision(7));
    if (!isXpand()) assertEquals(6, rsmd.getScale(7));
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
    if (!isXpand()) assertEquals(26, rs.getInt(columnSizeField));
    // timestamp(0)
    assertTrue(rs.next());
    assertEquals(19, rs.getInt(columnSizeField));
    // timestamp(6)
    assertTrue(rs.next());
    if (!isXpand()) assertEquals(26, rs.getInt(columnSizeField));
    // time(0)
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(columnSizeField));
    // time(6)
    assertTrue(rs.next());
    if (!isXpand()) assertEquals(17, rs.getInt(columnSizeField));

    assertFalse(rs.next());
  }

  /**
   * CONJ-401 - getProcedureColumns precision when server doesn't support precision.
   *
   * @throws SQLException if connection error occur
   */
  @Test
  public void metaTimeNoPrecisionProcedureResultSet() throws SQLException {
    Assumptions.assumeFalse(isXpand());
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE PROCEDURE getProcTimePrecision2(IN  I date, "
            + "IN t1 DATETIME,"
            + "IN t3 timestamp,"
            + "IN t5 time) BEGIN SELECT I; END");

    final int precisionField = 8;
    final int lengthField = 9;
    final int scaleField = 10;

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getProcedureColumns(null, null, "getProcTimePrecision2", null);
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
    Assumptions.assumeTrue(!isXpand());
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE PROCEDURE getProcTimePrecision"
            + "(IN  I date, "
            + "IN t1 DATETIME(0),"
            + "IN t2 DATETIME(6),"
            + "IN t3 timestamp(0),"
            + "IN t4 timestamp(6),"
            + "IN t5 time ,"
            + "IN t6 time(6)) BEGIN SELECT I; END");

    final int precisionField = 8;
    final int lengthField = 9;
    final int scaleField = 10;

    DatabaseMetaData dmd = sharedConn.getMetaData();
    ResultSet rs = dmd.getProcedureColumns(null, null, "getProcTimePrecision", null);
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
  public void various() {
    org.mariadb.jdbc.DatabaseMetaData meta = sharedConn.getMetaData();
    assertEquals(64, meta.getMaxProcedureNameLength());
  }

  @Test
  public void getIndexInfo() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();

    Common.assertThrowsContains(
        SQLException.class,
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
    assertEquals("A", rs.getString(10));
    assertEquals(0L, rs.getLong(11));
    assertNull(rs.getString(12));
    assertNull(rs.getString(13));

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
    assertEquals("A", rs.getString(10));
    assertEquals(0L, rs.getLong(11));
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
    assertEquals("A", rs.getString(10));
    assertEquals(0L, rs.getLong(11));
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
    assertEquals("A", rs.getString(10));
    assertEquals(0L, rs.getLong(11));
    assertNull(rs.getString(12));
    assertNull(rs.getString(13));
  }

  @Test
  public void getIndexInfoSchema() throws SQLException {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      DatabaseMetaData meta = con.getMetaData();

      Common.assertThrowsContains(
          SQLException.class,
          () -> meta.getIndexInfo(null, null, null, true, true),
          "'table' parameter must not be null");

      ResultSet rs = meta.getIndexInfo(null, null, "get_index_info", false, true);
      rs.next();
      assertEquals("def", rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("get_index_info", rs.getString(3));
      assertFalse(rs.getBoolean(4));
      assertEquals(con.getCatalog(), rs.getString(5));
      assertEquals("PRIMARY", rs.getString(6));
      assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
      assertEquals(1, rs.getShort(8));
      assertEquals("no", rs.getString(9));
      assertEquals("A", rs.getString(10));
      assertEquals(0L, rs.getLong(11));
      assertNull(rs.getString(12));
      assertNull(rs.getString(13));

      assertTrue(rs.next());
      assertEquals("def", rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("get_index_info", rs.getString(3));
      assertTrue(rs.getBoolean(4));
      assertEquals(con.getCatalog(), rs.getString(5));
      assertEquals("ind_cust", rs.getString(6));
      assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
      assertEquals(1, rs.getShort(8));
      assertEquals("customer_id", rs.getString(9));
      assertEquals("A", rs.getString(10));
      assertEquals(0L, rs.getLong(11));
      assertNull(rs.getString(12));
      assertNull(rs.getString(13));

      assertTrue(rs.next());
      assertEquals(con.getCatalog(), rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("get_index_info", rs.getString(3));
      assertTrue(rs.getBoolean(4));
      assertEquals(con.getCatalog(), rs.getString(5));
      assertEquals("ind_prod", rs.getString(6));
      assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
      assertEquals(1, rs.getShort(8));
      assertEquals("product_category", rs.getString(9));
      assertEquals("A", rs.getString(10));
      assertEquals(0L, rs.getLong(11));
      assertNull(rs.getString(12));
      assertNull(rs.getString(13));

      assertTrue(rs.next());
      assertEquals(con.getCatalog(), rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("get_index_info", rs.getString(3));
      assertTrue(rs.getBoolean(4));
      assertEquals(con.getCatalog(), rs.getString(5));
      assertEquals("ind_prod", rs.getString(6));
      assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
      assertEquals(2, rs.getShort(8));
      assertEquals("product_id", rs.getString(9));
      assertEquals("A", rs.getString(10));
      assertEquals(0L, rs.getLong(11));
      assertNull(rs.getString(12));
      assertNull(rs.getString(13));
    }
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
    assertTrue(meta.supportsSavepoints());
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
    Common.assertThrowsContains(
        SQLException.class,
        () -> meta.unwrap(String.class),
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
    assertEquals(isMariaDBServer() ? "MariaDB" : "MySQL", meta.getDatabaseProductName());
    assertEquals("MariaDB Connector/J", meta.getDriverName());
    assertTrue(meta.getDriverVersion().startsWith("3."));
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
    assertEquals(64, meta.getMaxColumnNameLength());
    assertEquals(64, meta.getMaxColumnsInGroupBy());
    assertEquals(16, meta.getMaxColumnsInIndex());
    assertEquals(64, meta.getMaxColumnsInOrderBy());
    assertEquals(Short.MAX_VALUE, meta.getMaxColumnsInSelect());
    assertEquals(0, meta.getMaxColumnsInTable());
    assertEquals(0, meta.getMaxConnections());
    assertEquals(0, meta.getMaxCursorNameLength());
    assertEquals(256, meta.getMaxIndexLength());
    assertEquals(0, meta.getMaxSchemaNameLength());
    assertEquals(64, meta.getMaxProcedureNameLength());
    assertEquals(0, meta.getMaxCatalogNameLength());
    assertEquals(0, meta.getMaxRowSize());
    assertFalse(meta.doesMaxRowSizeIncludeBlobs());
    assertEquals(0, meta.getMaxStatementLength());
    assertEquals(0, meta.getMaxStatements());
    assertEquals(64, meta.getMaxTableNameLength());
    assertEquals(256, meta.getMaxTablesInSelect());
    assertEquals(0, meta.getMaxUserNameLength());
    assertEquals(Connection.TRANSACTION_REPEATABLE_READ, meta.getDefaultTransactionIsolation());
    assertTrue(meta.supportsTransactions());
    assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
    assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
    assertTrue(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
    assertFalse(meta.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
    assertTrue(meta.supportsDataDefinitionAndDataManipulationTransactions());
    assertFalse(meta.supportsDataManipulationTransactionsOnly());
    assertTrue(meta.dataDefinitionCausesTransactionCommit());
    assertFalse(meta.dataDefinitionIgnoredInTransactions());
  }

  @Test
  public void testMetaCatalog() throws Exception {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getProcedures(sharedConn.getCatalog(), null, "testMetaCatalog");
    assertTrue(rs.next());
    assertEquals(sharedConn.getCatalog(), rs.getString(1));
    assertNull(rs.getString(2));
    assertEquals("testMetaCatalog", rs.getString(3));
    assertNull(rs.getString(4));
    assertNull(rs.getString(5));
    assertNull(rs.getString(6));
    assertEquals("comments", rs.getString(7));
    assertEquals(DatabaseMetaData.procedureNoResult, rs.getInt(8));
    assertEquals("testMetaCatalog", rs.getString(9));
    assertFalse(rs.next());

    // test with bad catalog
    rs = meta.getProcedures("yahoooo", null, "testMetaCatalog");
    assertFalse(rs.next());

    // test without catalog
    rs = meta.getProcedures(null, null, "testMetaCatalog");
    assertTrue(rs.next());
    assertEquals("testMetaCatalog", rs.getString(3));
    assertFalse(rs.next());
  }

  @Test
  public void testMetaCatalogSchema() throws Exception {
    try (Connection con = createCon("&useCatalogTerm=Schema")) {
      DatabaseMetaData meta = con.getMetaData();
      ResultSet rs = meta.getProcedures(null, con.getSchema(), "testMetaCatalog");
      assertTrue(rs.next());
      assertEquals("def", rs.getString(1));
      assertEquals(database, rs.getString(2));
      assertEquals("testMetaCatalog", rs.getString(3));
      assertNull(rs.getString(4));
      assertNull(rs.getString(5));
      assertNull(rs.getString(6));
      assertEquals("comments", rs.getString(7));
      assertEquals(DatabaseMetaData.procedureNoResult, rs.getInt(8));
      assertEquals("testMetaCatalog", rs.getString(9));
      assertFalse(rs.next());

      // test with bad catalog
      rs = meta.getProcedures(null, "yahoooo", "testMetaCatalog");
      assertFalse(rs.next());

      // test without catalog
      rs = meta.getProcedures(null, null, "testMetaCatalog");
      assertTrue(rs.next());
      assertEquals("testMetaCatalog", rs.getString(3));
      assertFalse(rs.next());
    }
  }

  @Test
  public void getMetaData() throws SQLException {
    ResultSet rs = sharedConn.createStatement().executeQuery("SELECT * FROM json_test");
    ResultSetMetaData meta = rs.getMetaData();
    assertTrue(
        "LONGTEXT".equals(meta.getColumnTypeName(1)) || "JSON".equals(meta.getColumnTypeName(1)));
    assertEquals(Types.LONGVARCHAR, meta.getColumnType(1));
    assertEquals("java.lang.String", meta.getColumnClassName(1));
  }

  @Test
  public void getTypeMetaData() throws SQLException {
    Assumptions.assumeTrue(!isXpand());
    //            "create table text_types_text (varchar100           varchar(100),\n" +
    //                    "  varchar255           varchar(255),\n" +
    //                    "  text                 text,\n" +
    //                    "  `tinytext`           tinytext,\n" +
    //                    "  `mediumtext`         mediumtext,\n" +
    //                    "  `longtext`           longtext)"
    try (ResultSet resultSet =
        sharedConn.createStatement().executeQuery("select * from text_types_text")) {
      ResultSetMetaData metaData = resultSet.getMetaData();

      String[] expected =
          new String[] {
            "varchar100 12 VARCHAR 100",
            "varchar255 12 VARCHAR 255",
            "text 12 TEXT 65535",
            "tinytext 12 VARCHAR 255",
            "mediumtext 12 MEDIUMTEXT 16777215",
            "longtext -1 LONGTEXT 0"
          };
      for (int i = 0; i < expected.length; i++) {
        assertEquals(
            expected[i],
            metaData.getColumnName(i + 1)
                + " "
                + metaData.getColumnType(i + 1)
                + " "
                + metaData.getColumnTypeName(i + 1)
                + " "
                + metaData.getPrecision(i + 1));
      }
    }
  }

  @Test
  public void foreignKeyTest() throws SQLException {

    try (Connection con = createCon()) {
      String db = con.getCatalog();
      java.sql.Statement stmt = con.createStatement();
      stmt.execute("DROP DATABASE IF EXISTS dbTmp");
      stmt.execute("DROP TABLE IF EXISTS tableWithForeignKey");
      stmt.execute("DROP TABLE IF EXISTS tableWithPk");
      stmt.execute("CREATE TABLE tableWithPk(id BIGINT PRIMARY KEY) ENGINE=InnoDB");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS tableWithForeignKey\n"
              + "          (\n"
              + "                  id BIGINT NOT NULL AUTO_INCREMENT,\n"
              + "                  fk_id BIGINT,\n"
              + "                  PRIMARY KEY (id),\n"
              + "  CONSTRAINT fk_key FOREIGN KEY (fk_id) REFERENCES tableWithPk (id)\n"
              + "          ) ENGINE=InnoDB");
      DatabaseMetaData dbmd = con.getMetaData();
      ResultSet rs = dbmd.getImportedKeys(null, null, "tableWithForeignKey");
      assertTrue(rs.next());
      rs.close();

      rs = dbmd.getImportedKeys("", null, "tableWithForeignKey");
      assertTrue(rs.next());

      stmt.execute("CREATE DATABASE dbTmp");
      con.setCatalog("dbTmp");
      try {
        rs = dbmd.getImportedKeys("", null, "tableWithForeignKey");
        assertFalse(rs.next());

        rs = dbmd.getImportedKeys(null, null, "tableWithForeignKey");
        assertTrue(rs.next());
      } finally {
        con.setCatalog(db);
      }

      try (Connection con2 = createCon("&useCatalogTerm=Schema")) {
        dbmd = con.getMetaData();
        rs = dbmd.getImportedKeys(null, null, "tableWithForeignKey");
        assertTrue(rs.next());
        rs.close();

        rs = dbmd.getImportedKeys(null, "", "tableWithForeignKey");
        assertTrue(rs.next());

        con2.setCatalog("dbTmp");
        try {
          rs = dbmd.getImportedKeys(null, "", "tableWithForeignKey");
          assertTrue(rs.next());

          rs = dbmd.getImportedKeys(null, null, "tableWithForeignKey");
          assertTrue(rs.next());
        } finally {
          con2.setCatalog(db);
          stmt.execute("DROP DATABASE dbTmp");
        }
      }
    }
  }

  @Test
  public void testTemporaryTables() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(11, 2, 0));
    Assumptions.assumeTrue(
        !"mariadb-es".equals(System.getenv("srv"))
            && !"mariadb-es-test".equals(System.getenv("srv")));

    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("create temporary table testTemporaryTables (b int)");
    try (Connection con = createCon()) {
      java.sql.Statement ss = con.createStatement();
      ResultSet rr =
          ss.executeQuery(
              "select table_schema, table_name, table_type, temporary "
                  + "from information_schema.tables "
                  + "where TABLE_NAME = 'testTemporaryTables'");
      assertFalse(rr.next());
    }
    DatabaseMetaData dbmd = sharedConn.getMetaData();

    ResultSet rs = dbmd.getTables(null, null, "testTemporary%", null);
    assertTrue(rs.next());
    assertEquals("testtemporarytables", rs.getString("TABLE_NAME").toLowerCase());
    assertEquals("LOCAL TEMPORARY", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());

    rs = dbmd.getTables(null, null, "testTemporary%", new String[] {"LOCAL TEMPORARY"});
    assertTrue(rs.next());
    assertEquals("testtemporarytables", rs.getString("TABLE_NAME").toLowerCase());
    assertEquals("LOCAL TEMPORARY", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());

    rs = dbmd.getTables(null, null, "testTemporary%", new String[] {"TEMPORARY"});
    assertTrue(rs.next());
    assertEquals("testtemporarytables", rs.getString("TABLE_NAME").toLowerCase());
    assertEquals("LOCAL TEMPORARY", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());

    rs = dbmd.getTables(null, null, "testTemporary%", new String[] {"TABLE"});
    assertFalse(rs.next());

    rs = dbmd.getTables(null, null, "testTemporary%", new String[] {"TEMPORARY", "TABLE"});
    assertTrue(rs.next());
    assertEquals("testtemporarytables", rs.getString("TABLE_NAME").toLowerCase());
    assertEquals("LOCAL TEMPORARY", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());

    rs = dbmd.getTables(null, null, "testTemporary%", new String[] {"TEMPORARY", null, "TABLE"});
    assertTrue(rs.next());
    assertEquals("testtemporarytables", rs.getString("TABLE_NAME").toLowerCase());
    assertEquals("LOCAL TEMPORARY", rs.getString("TABLE_TYPE"));
    assertFalse(rs.next());
  }
}
