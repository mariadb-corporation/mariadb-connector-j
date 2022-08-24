// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.integration.resultset;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.tidb.jdbc.Statement;
import org.tidb.jdbc.integration.Common;

public class ResultSetMetadataTest extends Common {

  private static final Class<? extends java.lang.Exception> sqle = SQLException.class;

  @AfterAll
  public static void dropAll() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS ResultSetTest");
    stmt.execute("DROP TABLE IF EXISTS test_rsmd");
    stmt.execute("DROP TABLE IF EXISTS resultsetmetadatatest1");
    stmt.execute("DROP TABLE IF EXISTS resultsetmetadatatest2");
    stmt.execute("DROP TABLE IF EXISTS resultsetmetadatatest3");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    dropAll();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE ResultSetTest (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("INSERT INTO ResultSetTest(t2) values (1),(2),(3),(4),(5),(6),(7),(8)");
    stmt.execute(
        "CREATE TABLE test_rsmd(id_col int not null primary key auto_increment, "
            + "nullable_col varchar(20), unikey_col int unique, char_col char(10), us smallint unsigned)");
    stmt.execute("CREATE TABLE resultsetmetadatatest1(id int, name varchar(20))");
    stmt.execute("CREATE TABLE resultsetmetadatatest2(id int, name varchar(20))");
    stmt.execute("CREATE TABLE resultsetmetadatatest3(id int, name varchar(20))");
  }

  @Test
  public void metaDataTest() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("insert into test_rsmd (id_col,nullable_col,unikey_col) values (null, 'hej', 9)");
    ResultSet rs =
        stmt.executeQuery(
            "select id_col, nullable_col, unikey_col as something, char_col,us from test_rsmd");
    assertTrue(rs.next());
    ResultSetMetaData rsmd = rs.getMetaData();
    assertTrue(rsmd.isAutoIncrement(1));
    assertFalse(rsmd.isAutoIncrement(2));
    assertEquals(5, rsmd.getColumnCount());
    assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
    assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
    assertEquals(String.class.getName(), rsmd.getColumnClassName(2));
    assertEquals(Integer.class.getName(), rsmd.getColumnClassName(1));
    assertEquals(Integer.class.getName(), rsmd.getColumnClassName(3));
    assertEquals("id_col", rsmd.getColumnLabel(1));
    assertEquals("nullable_col", rsmd.getColumnLabel(2));
    assertEquals("something", rsmd.getColumnLabel(3));
    assertEquals("unikey_col", rsmd.getColumnName(3));
    assertEquals(Types.CHAR, rsmd.getColumnType(4));
    assertEquals(Types.INTEGER, rsmd.getColumnType(5));
    assertFalse(rsmd.isReadOnly(1));
    assertFalse(rsmd.isReadOnly(2));
    assertTrue(rsmd.isWritable(1));
    assertTrue(rsmd.isDefinitelyWritable(1));
    assertTrue(rsmd.isCaseSensitive(1));
    assertTrue(rsmd.isSearchable(1));
    assertFalse(rsmd.isCurrency(1));
    assertTrue(rsmd.isSigned(3));
    assertFalse(rsmd.isSigned(5));

    Common.assertThrowsContains(sqle, () -> rsmd.isAutoIncrement(6), "wrong column index 6");
    Common.assertThrowsContains(sqle, () -> rsmd.isReadOnly(6), "wrong column index 6");
    Common.assertThrowsContains(sqle, () -> rsmd.isReadOnly(-6), "wrong column index -6");
    Common.assertThrowsContains(sqle, () -> rsmd.isWritable(6), "wrong column index 6");
    Common.assertThrowsContains(sqle, () -> rsmd.isDefinitelyWritable(6), "wrong column index 6");

    DatabaseMetaData md = sharedConn.getMetaData();
    ResultSet cols = md.getColumns(null, null, "test\\_rsmd", null);
    cols.next();
    assertEquals("id_col", cols.getString("COLUMN_NAME"));
    assertEquals(Types.INTEGER, cols.getInt("DATA_TYPE"));
    cols.next(); /* nullable_col */
    cols.next(); /* unikey_col */
    cols.next(); /* char_col */
    assertEquals("char_col", cols.getString("COLUMN_NAME"));
    assertEquals(Types.CHAR, cols.getInt("DATA_TYPE"));
    cols.next(); /* us */ // CONJ-96: SMALLINT UNSIGNED gives Types.SMALLINT
    assertEquals("us", cols.getString("COLUMN_NAME"));
    assertEquals(Types.SMALLINT, cols.getInt("DATA_TYPE"));

    rs = stmt.executeQuery("select 1 from test_rsmd");
    ResultSetMetaData rsmd2 = rs.getMetaData();
    // TiDB not support read only selection
    assertFalse(rsmd2.isReadOnly(1));
    assertTrue(rsmd2.isWritable(1));
    assertTrue(rsmd2.isDefinitelyWritable(1));
  }

  @Test
  public void conj17() throws Exception {
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery("select count(*),1 from information_schema.tables");
    assertTrue(rs.next());
    assertEquals(rs.getMetaData().getColumnName(1), "count(*)");
    assertEquals(rs.getMetaData().getColumnName(2), "1");
  }

  @Test
  public void conj84() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("INSERT INTO resultsetmetadatatest1 VALUES (1, 'foo')");
    stmt.execute("INSERT INTO resultsetmetadatatest2 VALUES (2, 'bar')");
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery(
                "select resultsetmetadatatest1.*, resultsetmetadatatest2.* FROM resultsetmetadatatest1 join resultsetmetadatatest2");
    assertTrue(rs.next());
    assertEquals(rs.findColumn("id"), 1);
    assertEquals(rs.findColumn("name"), 2);
    assertEquals(rs.findColumn("resultsetmetadatatest1.id"), 1);
    assertEquals(rs.findColumn("resultsetmetadatatest1.name"), 2);
    assertEquals(rs.findColumn("resultsetmetadatatest2.id"), 3);
    assertEquals(rs.findColumn("resultsetmetadatatest2.name"), 4);
  }

  @Test
  public void testAlias() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testAlias");
    stmt.execute("DROP TABLE IF EXISTS testAlias2");
    stmt.execute("CREATE TABLE testAlias(id int, name varchar(20))");
    stmt.execute("CREATE TABLE testAlias2(id2 int, name2 varchar(20))");

    stmt.execute("INSERT INTO testAlias VALUES (1, 'foo')");
    stmt.execute("INSERT INTO testAlias2 VALUES (2, 'bar')");
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery(
                "select alias1.id as idalias1, "
                    + "alias1.name as namealias1, "
                    + "id2 as idalias2, "
                    + "name2, "
                    + "testAlias.id,"
                    + "alias1.id "
                    + "FROM testAlias as alias1 "
                    + "join testAlias2 as alias2 "
                    + "join testAlias");
    assertTrue(rs.next());

    assertEquals(rs.findColumn("idalias1"), 1);
    assertEquals(rs.findColumn("alias1.idalias1"), 1);

    Common.assertThrowsContains(sqle, () -> rs.findColumn("name"), "Unknown label 'name'");
    assertEquals(rs.findColumn("namealias1"), 2);
    assertEquals(rs.findColumn("alias1.namealias1"), 2);

    Common.assertThrowsContains(sqle, () -> rs.findColumn("id2"), "Unknown label 'id2'");
    assertEquals(rs.findColumn("idalias2"), 3);
    assertEquals(rs.findColumn("alias2.idalias2"), 3);
    Common.assertThrowsContains(
        sqle, () -> rs.findColumn("testAlias2.id2"), "Unknown label 'testAlias2.id2'");

    assertEquals(rs.findColumn("name2"), 4);
    Common.assertThrowsContains(
        sqle, () -> rs.findColumn("testAlias2.name2"), "Unknown label 'testAlias2.name2'");
    assertEquals(rs.findColumn("alias2.name2"), 4);

    assertEquals(rs.findColumn("id"), 5);
    assertEquals(rs.findColumn("testAlias.id"), 5);
    assertEquals(rs.findColumn("alias1.id"), 6);

    Common.assertThrowsContains(
        sqle, () -> rs.findColumn("alias2.name22"), "Unknown label 'alias2.name22'");
    Common.assertThrowsContains(sqle, () -> rs.findColumn(""), "Unknown label ''");
    Common.assertThrowsContains(sqle, () -> rs.findColumn(null), "null is not a valid label value");
  }

  @Test
  public void blankTableNameMeta() throws Exception {
    ResultSet rs =
        sharedConn
            .createStatement()
            .executeQuery(
                "SELECT id AS id_alias FROM resultsetmetadatatest3 AS resultsetmetadatatest1_alias");
    ResultSetMetaData rsmd = rs.getMetaData();

    assertEquals("resultsetmetadatatest3", rsmd.getTableName(1));
    assertEquals(rsmd.getColumnLabel(1), "id_alias");
    assertEquals(rsmd.getColumnName(1), "id");

    try (Connection connection = createCon("&blankTableNameMeta")) {
      rs =
          connection
              .createStatement()
              .executeQuery(
                  "SELECT id AS id_alias FROM resultsetmetadatatest3 AS resultsetmetadatatest1_alias");
      rsmd = rs.getMetaData();

      assertEquals("", rsmd.getTableName(1));
      assertEquals("id_alias", rsmd.getColumnLabel(1));
      assertEquals("id", rsmd.getColumnName(1));
    }
  }

  @Test
  public void staticMethod() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT * FROM resultsetmetadatatest3");
    ResultSetMetaData rsmd = rs.getMetaData();

    rsmd.unwrap(org.tidb.jdbc.client.result.ResultSetMetaData.class);

    Common.assertThrowsContains(
        SQLException.class,
        () -> rsmd.unwrap(String.class),
        "The receiver is not a wrapper for java.lang.String");
  }

  @Test
  public void databaseResultsetMeta() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();
    ResultSet rs = meta.getTableTypes();
    assertTrue(rs.next());
    ResultSetMetaData rsMeta = rs.getMetaData();
    assertEquals("TABLE_TYPE", rsMeta.getColumnName(1));
    assertEquals("", rsMeta.getTableName(1));
  }
}
