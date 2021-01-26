/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import org.junit.jupiter.api.*;
import org.mariadb.jdbc.Common;

public class UpdateResultSetTest extends Common {

  /**
   * Test error message when no primary key.
   *
   * @throws Exception not expected
   */
  @Test
  public void testNoPrimaryKey() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testnoprimarykey");
    stmt.execute("CREATE TABLE testnoprimarykey(`id` INT NOT NULL,`t1` VARCHAR(50) NOT NULL)");
    stmt.execute("INSERT INTO testnoprimarykey VALUES (1, 't1'), (2, 't2')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT * FROM testnoprimarykey",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertThrowsContains(
          SQLException.class,
          () -> rs.updateString(1, "1"),
          "ResultSet cannot be updated. Cannot update rows, since primary field is not present in query");
    }
  }

  @Test
  public void testNoDatabase() throws Exception {
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertThrowsContains(
          SQLException.class,
          () -> rs.updateString(1, "1"),
          "The result-set contains fields without without any database information");
    }
  }

  @Test
  public void testMultipleTable() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testMultipleTable1");
    stmt.execute("DROP TABLE IF EXISTS testMultipleTable2");
    stmt.execute(
        "CREATE TABLE testMultipleTable1(`id1` INT NOT NULL AUTO_INCREMENT,`t1` VARCHAR(50) NULL,PRIMARY KEY (`id1`))");
    stmt.execute(
        "CREATE TABLE testMultipleTable2(`id2` INT NOT NULL AUTO_INCREMENT,`t1` VARCHAR(50) NULL,PRIMARY KEY (`id2`))");

    stmt.executeQuery("INSERT INTO testMultipleTable1(t1) values ('1')");
    stmt.executeQuery("INSERT INTO testMultipleTable2(t1) values ('2')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT * FROM testMultipleTable1, testMultipleTable2",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertThrowsContains(
          SQLException.class,
          () -> rs.updateString("t1", "new value"),
          "ResultSet cannot be updated. " + "The result-set contains fields on different tables");
    }
  }

  @Test
  public void testOneNoTable() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testOneNoTable");
    stmt.execute(
        "CREATE TABLE testOneNoTable(`id1` INT NOT NULL AUTO_INCREMENT,`t1` VARCHAR(50) NULL,PRIMARY KEY (`id1`))");
    stmt.executeQuery("INSERT INTO testOneNoTable(t1) values ('1')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT *, now() FROM testOneNoTable",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertThrowsContains(
          SQLException.class,
          () -> rs.updateString("t1", "new value"),
          "ResultSet cannot be updated. "
              + "The result-set contains fields without without any database information");
    }
  }

  @Test
  public void testMultipleDatabase() throws Exception {
    Statement stmt = sharedConn.createStatement();
    try {
      stmt.execute("DROP DATABASE testConnectorJ");
    } catch (SQLException sqle) {
      // eat
    }
    stmt.execute("CREATE DATABASE testConnectorJ");
    stmt.execute("DROP TABLE IF EXISTS testMultipleDatabase");
    stmt.execute(
        "CREATE TABLE testMultipleDatabase(`id1` INT NOT NULL AUTO_INCREMENT,`t1` VARCHAR(50) NULL,PRIMARY KEY (`id1`))");
    stmt.execute(
        "CREATE TABLE testConnectorJ.testMultipleDatabase(`id2` INT NOT NULL AUTO_INCREMENT,`t2` VARCHAR(50) NULL,PRIMARY KEY (`id2`))");
    stmt.executeQuery("INSERT INTO testMultipleDatabase(t1) values ('1')");
    stmt.executeQuery("INSERT INTO testConnectorJ.testMultipleDatabase(t2) values ('2')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT * FROM "
                + sharedConn.getCatalog()
                + ".testMultipleDatabase, testConnectorJ.testMultipleDatabase",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      assertThrowsContains(
          SQLException.class,
          () -> rs.updateString("t1", "new value"),
          "The result-set contains more than one database");
    }
    try {
      stmt.execute("DROP DATABASE testConnectorJ");
    } catch (SQLException sqle) {
      // eat
    }
  }

  @Test
  public void testMeta() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS UpdateWithoutPrimary");
    stmt.execute(
        "CREATE TABLE UpdateWithoutPrimary(`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL,"
            + "`t2` VARCHAR(50) NULL default 'default-value',"
            + "PRIMARY KEY (`id`))");
    stmt.executeQuery("INSERT INTO UpdateWithoutPrimary(t1,t2) values ('1-1','1-2')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT t1, t2 FROM UpdateWithoutPrimary",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());

      assertThrowsContains(
          SQLException.class,
          () -> {
            rs.updateString(1, "1-1-bis");
            rs.updateRow();
          },
          "ResultSet cannot be updated. Cannot update rows, since primary field is not present in query");
      assertThrowsContains(
          SQLException.class,
          () -> rs.deleteRow(),
          "ResultSet cannot be updated. Cannot update rows, since primary field is not present in query");
      ResultSetMetaData rsmd = rs.getMetaData();
      assertFalse(rsmd.isReadOnly(1));
      assertFalse(rsmd.isReadOnly(2));
      assertTrue(rsmd.isWritable(1));
      assertTrue(rsmd.isWritable(2));
      assertTrue(rsmd.isDefinitelyWritable(1));
      assertTrue(rsmd.isDefinitelyWritable(2));

      assertThrowsContains(SQLException.class, () -> rsmd.isReadOnly(3), "no column with index 3");
      assertThrowsContains(SQLException.class, () -> rsmd.isWritable(3), "no column with index 3");
      assertThrowsContains(
          SQLException.class, () -> rsmd.isDefinitelyWritable(3), "no column with index 3");
    }
    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM UpdateWithoutPrimary");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1-1", rs.getString(2));
    assertEquals("1-2", rs.getString(3));

    assertFalse(rs.next());
  }

  @Test
  public void testUpdateWhenFetch() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testUpdateWhenFetch");
    stmt.execute(
        "CREATE TABLE testUpdateWhenFetch("
            + "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL,"
            + "`t2` VARCHAR(50) NULL default 'default-value',"
            + "PRIMARY KEY (`id`))"
            + "DEFAULT CHARSET=utf8");

    PreparedStatement pstmt =
        sharedConn.prepareStatement("INSERT INTO testUpdateWhenFetch(t1,t2) values (?, ?)");
    for (int i = 1; i < 100; i++) {
      pstmt.setString(1, i + "-1");
      pstmt.setString(2, i + "-2");
      pstmt.addBatch();
    }
    pstmt.executeBatch();

    String utf8escapeQuote = "你好 '' \" \\";
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT id, t1, t2 FROM testUpdateWhenFetch",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      preparedStatement.setFetchSize(2);
      ResultSet rs = preparedStatement.executeQuery();

      rs.moveToInsertRow();
      rs.updateInt(1, -1);
      rs.updateString(2, "0-1");
      rs.updateString(3, "0-2");
      rs.insertRow();

      rs.next();
      rs.next();
      rs.updateString(2, utf8escapeQuote);
      rs.updateRow();
    }

    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM testUpdateWhenFetch");
    assertTrue(rs.next());
    assertEquals(-1, rs.getInt(1));
    assertEquals("0-1", rs.getString(2));
    assertEquals("0-2", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1-1", rs.getString(2));
    assertEquals("1-2", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(utf8escapeQuote, rs.getString(2));
    assertEquals("2-2", rs.getString(3));

    for (int i = 3; i < 100; i++) {
      assertTrue(rs.next());
      assertEquals(i + "-1", rs.getString(2));
      assertEquals(i + "-2", rs.getString(3));
    }
    assertFalse(rs.next());
  }

  @Test
  public void testPrimaryGenerated() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS PrimaryGenerated");
    stmt.execute(
        "CREATE TABLE PrimaryGenerated("
            + "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL,"
            + "`t2` VARCHAR(50) NULL default 'default-value',"
            + "PRIMARY KEY (`id`))");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT t1, t2, id FROM PrimaryGenerated",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertFalse(rs.next());

      rs.moveToInsertRow();
      rs.updateString(1, "1-1");
      rs.updateString(2, "1-2");
      rs.insertRow();

      rs.moveToInsertRow();
      rs.updateString(1, "2-1");
      rs.insertRow();

      rs.moveToInsertRow();
      rs.updateString(2, "3-2");
      assertThrows(SQLException.class, () -> rs.insertRow());

      rs.absolute(1);
      assertEquals("1-1", rs.getString(1));
      assertEquals("1-2", rs.getString(2));
      assertEquals(1, rs.getInt(3));

      assertTrue(rs.next());
      assertEquals("2-1", rs.getString(1));
      assertEquals("default-value", rs.getString(2));
      assertEquals(2, rs.getInt(3));

      assertFalse(rs.next());
    }

    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM PrimaryGenerated");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("1-1", rs.getString(2));
    assertEquals("1-2", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("2-1", rs.getString(2));
    assertEquals("default-value", rs.getString(3));

    assertFalse(rs.next());
  }

  @Test
  public void testPrimaryGeneratedDefault() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testPrimaryGeneratedDefault");
    stmt.execute(
        "CREATE TABLE testPrimaryGeneratedDefault("
            + "`id` INT NOT NULL AUTO_INCREMENT,"
            + "`t1` VARCHAR(50) NOT NULL default 'default-value1',"
            + "`t2` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
            + "PRIMARY KEY (`id`))");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT id, t1, t2 FROM testPrimaryGeneratedDefault",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertFalse(rs.next());
      rs.moveToInsertRow();
      rs.insertRow();

      rs.moveToInsertRow();
      rs.insertRow();
      rs.beforeFirst();

      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("default-value1", rs.getString(2));
      assertNotNull(rs.getDate(3));

      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("default-value1", rs.getString(2));
      assertNotNull(rs.getDate(3));
      assertFalse(rs.next());
    }

    ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM testPrimaryGeneratedDefault");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("default-value1", rs.getString(2));
    assertNotNull(rs.getDate(3));

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals("default-value1", rs.getString(2));
    assertNotNull(rs.getDate(3));

    assertFalse(rs.next());
  }

  @Test
  public void testDelete() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testDelete");
    stmt.execute(
        "CREATE TABLE testDelete("
            + "`id` INT NOT NULL,"
            + "`id2` INT NOT NULL,"
            + "`t1` VARCHAR(50),"
            + "PRIMARY KEY (`id`,`id2`))");

    stmt = sharedConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    stmt.execute("INSERT INTO testDelete values (1,-1,'1'), (2,-2,'2'), (3,-3,'3')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT * FROM testDelete", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertThrows(SQLException.class, () -> rs.deleteRow());
      assertTrue(rs.next());
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      rs.deleteRow();
      assertEquals(1, rs.getInt(1));
      assertEquals(-1, rs.getInt(2));
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      assertEquals(-3, rs.getInt(2));
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM testDelete");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(-1, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(-3, rs.getInt(2));
    assertFalse(rs.next());

    rs.absolute(1);
    rs.deleteRow();
    assertThrowsContains(SQLException.class, () -> rs.getInt(1), "wrong row position");
  }

  @Test
  public void testUpdateChangingMultiplePrimaryKey() throws Exception {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testUpdateChangingMultiplePrimaryKey");
    stmt.execute(
        "CREATE TABLE testUpdateChangingMultiplePrimaryKey("
            + "`id` INT NOT NULL,"
            + "`id2` INT NOT NULL,"
            + "`t1` VARCHAR(50),"
            + "PRIMARY KEY (`id`,`id2`))");
    stmt.execute(
        "INSERT INTO testUpdateChangingMultiplePrimaryKey values (1,-1,'1'), (2,-2,'2'), (3,-3,'3')");
    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT * FROM testUpdateChangingMultiplePrimaryKey",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.next());
      assertTrue(rs.next());
      rs.updateInt(1, 4);
      rs.updateInt(2, -4);
      rs.updateString(3, "4");
      rs.updateRow();

      assertEquals(4, rs.getInt(1));
      assertEquals(-4, rs.getInt(2));
      assertEquals("4", rs.getString(3));
    }

    ResultSet rs = stmt.executeQuery("SELECT * FROM testUpdateChangingMultiplePrimaryKey");

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(-1, rs.getInt(2));
    assertEquals("1", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(-3, rs.getInt(2));
    assertEquals("3", rs.getString(3));

    assertTrue(rs.next());
    assertEquals(4, rs.getInt(1));
    assertEquals(-4, rs.getInt(2));
    assertEquals("4", rs.getString(3));

    assertFalse(rs.next());
  }

  @Test
  public void updateBlob() throws SQLException, IOException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS updateBlob");
    stmt.execute("CREATE TABLE updateBlob(id int not null primary key, strm blob)");
    PreparedStatement prep =
        sharedConn.prepareStatement("insert into updateBlob (id, strm) values (?,?)");
    byte[] theBlob = {1, 2, 3, 4, 5, 6};
    InputStream stream = new ByteArrayInputStream(theBlob);

    prep.setInt(1, 1);
    prep.setBlob(2, stream);
    prep.execute();

    byte[] updatedBlob = {1, 3, 6, 9, 15, 21};

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "select * from updateBlob", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      InputStream updatedStream = new ByteArrayInputStream(updatedBlob);

      rs.updateBlob(2, updatedStream);
      rs.updateRow();

      checkResult(rs, updatedBlob);
    }

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "select * from updateBlob", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();
      assertTrue(rs.next());
      checkResult(rs, updatedBlob);
    }
  }

  private void checkResult(ResultSet rs, byte[] updatedBlob) throws SQLException, IOException {
    InputStream readStuff = rs.getBlob("strm").getBinaryStream();
    int ch;
    int pos = 0;
    while ((ch = readStuff.read()) != -1) {
      assertEquals(updatedBlob[pos++], ch);
    }

    readStuff = rs.getBinaryStream("strm");

    pos = 0;
    while ((ch = readStuff.read()) != -1) {
      assertEquals(updatedBlob[pos++], ch);
    }
  }

  @Test
  public void updateMeta() throws SQLException {
    DatabaseMetaData meta = sharedConn.getMetaData();

    assertTrue(meta.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
    assertTrue(meta.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
    assertTrue(meta.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
    assertTrue(
        meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertTrue(
        meta.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));

    assertTrue(meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
    assertTrue(meta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
    assertTrue(meta.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
    assertTrue(
        meta.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
    assertTrue(
        meta.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));

    assertFalse(meta.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
    assertFalse(meta.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
    assertFalse(meta.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
    assertFalse(
        meta.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
    assertFalse(
        meta.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));
  }

  @Test
  public void updateResultSetMeta() throws SQLException {
    java.sql.Statement stmt = sharedConn.createStatement();
    assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    ResultSet rs = stmt.executeQuery("SELECT 1");
    assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

    stmt = sharedConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
    assertEquals(ResultSet.CONCUR_UPDATABLE, stmt.getResultSetConcurrency());
    rs = stmt.executeQuery("SELECT 1");
    assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
  }

  @Test
  public void insertNoRow() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS insertNoRow");
    stmt.execute("CREATE TABLE insertNoRow(id int not null primary key, strm blob)");
    java.sql.Statement st =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = st.executeQuery("select * from insertNoRow");
    assertFalse(rs.next());
    rs.moveToInsertRow();
    try {
      rs.refreshRow();
      fail("Can't refresh when on the insert row.");
    } catch (SQLException sqle) {
      // expected
    }
    rs.moveToCurrentRow();
  }

  @Test
  public void refreshRow() throws SQLException {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      refreshRow(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      refreshRow(con);
    }
  }

  private void refreshRow(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS refreshRow");
    stmt.execute("CREATE TABLE refreshRow(id int not null primary key, strm text)");

    java.sql.Statement st =
        con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
    st.execute("INSERT INTO refreshRow values (1, '555')");
    ResultSet rs = st.executeQuery("select * from refreshRow");

    st.execute("UPDATE refreshRow set strm = '666' WHERE id = 1");
    try {
      rs.refreshRow();
      fail("Can't refresh when not on row.");
    } catch (SQLException sqle) {
      // expected
    }

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals("555", rs.getString(2));
    rs.refreshRow();
    assertEquals("666", rs.getString(2));

    rs.moveToInsertRow();
    try {
      rs.refreshRow();
      fail("Can't refresh when on insert row");
    } catch (SQLException sqle) {
      // expected
    }
    rs.moveToCurrentRow();

    assertFalse(rs.next());
    try {
      rs.refreshRow();
      fail("Can't refresh when not on row.");
    } catch (SQLException sqle) {
      // expected
    }
  }

  @Test
  public void testMoveToInsertRow() throws SQLException {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      testMoveToInsertRow(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      testMoveToInsertRow(con);
    }
  }

  private void testMoveToInsertRow(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS testMoveToInsertRow");
    stmt.execute("CREATE TABLE testMoveToInsertRow(t2 text, t1 text, id int primary key)");

    try (PreparedStatement preparedStatement =
        con.prepareStatement(
            "select id, t1, t2 from testMoveToInsertRow",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertNotNull(rs);
      assertEquals(0, rs.getRow());
      rs.moveToInsertRow();
      rs.updateInt(1, 1);
      rs.updateString(2, "t1-value");
      rs.updateString(3, "t2-value");
      rs.insertRow();
      rs.first();
      assertEquals(1, rs.getRow());

      rs.updateInt("id", 2);
      rs.updateString("t1", "t1-bis-value");
      rs.updateRow();
      assertEquals(1, rs.getRow());

      assertEquals(2, rs.getInt("id"));
      assertEquals("t1-bis-value", rs.getString("t1"));
      assertEquals("t2-value", rs.getString("t2"));

      rs.deleteRow();
      assertEquals(0, rs.getRow());

      rs.moveToInsertRow();
      rs.updateInt("id", 3);
      rs.updateString("t1", "other-t1-value");

      rs.insertRow();
      assertEquals(0, rs.getRow());
      assertThrowsContains(
          SQLException.class,
          () -> rs.refreshRow(),
          "Cannot call deleteRow() when inserting a new row");
      rs.next();
      assertEquals(3, rs.getInt("id"));
      assertEquals("other-t1-value", rs.getString("t1"));
      assertNull(rs.getString("t2"));
    }

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "select id, t1, t2 from testMoveToInsertRow",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.first());
      assertEquals(1, rs.getRow());
      rs.updateInt("id", 3);
      rs.updateString("t1", "t1-3");
      rs.updateRow();
      assertEquals(1, rs.getRow());

      assertEquals(3, rs.getInt("id"));
      assertEquals("t1-3", rs.getString("t1"));

      rs.moveToInsertRow();
      rs.updateInt("id", 4);
      rs.updateString("t1", "t1-4");
      rs.insertRow();
      assertEquals(1, rs.getRow());

      rs.updateInt("id", 5);
      rs.updateString("t1", "t1-5");
      rs.insertRow();
      assertEquals(1, rs.getRow());

      rs.moveToCurrentRow();
      assertEquals(3, rs.getInt("id"));
      assertEquals("t1-3", rs.getString("t1"));

      assertTrue(rs.next());
      assertEquals(4, rs.getInt("id"));
      assertEquals("t1-4", rs.getString("t1"));

      assertTrue(rs.next());
      assertEquals(5, rs.getInt("id"));
      assertEquals("t1-5", rs.getString("t1"));
    }
  }

  @Test
  public void cancelRowUpdatesTest() throws SQLException {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      cancelRowUpdatesTest(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      cancelRowUpdatesTest(con);
    }
  }

  private void cancelRowUpdatesTest(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS cancelRowUpdatesTest");
    stmt.execute("CREATE TABLE cancelRowUpdatesTest(c text, id int primary key)");
    stmt.execute("INSERT INTO cancelRowUpdatesTest(id,c) values (1,'1'), (2,'2'),(3,'3'),(4,'4')");

    try (PreparedStatement preparedStatement =
        con.prepareStatement(
            "select id,c from cancelRowUpdatesTest order by id",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.next());
      assertTrue(rs.next());

      assertEquals("2", rs.getString("c"));
      rs.updateString("c", "2bis");
      rs.cancelRowUpdates();
      rs.updateRow();
      assertEquals("2", rs.getString("c"));

      rs.updateString("c", "2bis");
      rs.updateRow();
      assertEquals("2bis", rs.getString("c"));

      assertTrue(rs.first());
      assertTrue(rs.next());
      assertEquals("2bis", rs.getString("c"));
    }
  }

  @Test
  public void deleteRowsTest() throws SQLException {
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts=false")) {
      deleteRowsTest(con);
    }
    try (org.mariadb.jdbc.Connection con = createCon("&useServerPrepStmts")) {
      deleteRowsTest(con);
    }
  }

  private void deleteRowsTest(Connection con) throws SQLException {
    Statement stmt = con.createStatement();
    stmt.execute("DROP TABLE IF EXISTS deleteRows");
    stmt.execute("CREATE TABLE deleteRows(c text, id int primary key)");
    stmt.execute("INSERT INTO deleteRows(id,c) values (1,'1'), (2,'2'),(3,'3'),(4,'4')");

    try (PreparedStatement preparedStatement =
        con.prepareStatement(
            "select id,c from deleteRows order by id",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = preparedStatement.executeQuery();

      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));

      rs.deleteRow();

      assertTrue(rs.isBeforeFirst());

      assertTrue(rs.next());
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("id"));

      rs.deleteRow();
      assertEquals(2, rs.getInt("id"));
    }
  }

  @Test
  public void updatePosTest() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS updatePosTest");
    stmt.execute("CREATE TABLE updatePosTest(c text, id int primary key)");
    stmt.execute("INSERT INTO updatePosTest(id,c) values (1,'1')");

    try (PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "select id,c from updatePosTest",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE)) {

      ResultSet rs = preparedStatement.executeQuery();
      assertThrowsContains(
          SQLException.class,
          () -> rs.updateInt(1, 20),
          "Current position is before the first row");
      assertThrowsContains(
          SQLException.class, () -> rs.updateRow(), "Current position is before the first row");
      assertThrowsContains(
          SQLException.class, () -> rs.deleteRow(), "Current position is before the first row");

      assertTrue(rs.next());
      rs.updateInt(1, 20);
      rs.updateNull(2);
      rs.updateRow();
      rs.deleteRow();
      assertFalse(rs.next());
      assertThrowsContains(
          SQLException.class, () -> rs.updateInt(1, 20), "Current position is after the last row");
      assertThrowsContains(
          SQLException.class, () -> rs.updateRow(), "Current position is after the last row");
      assertThrowsContains(
          SQLException.class, () -> rs.deleteRow(), "Current position is after the last row");
    }
  }

  /**
   * CONJ-519 : Updatable result-set possible NPE when same field is repeated.
   *
   * @throws SQLException if any exception occur
   */
  @Test
  public void repeatedFieldUpdatable() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS repeatedFieldUpdatable");
    stmt.execute(
        "CREATE TABLE repeatedFieldUpdatable(t1 varchar(50) NOT NULL, t2 varchar(50), PRIMARY KEY (t1))");
    stmt.execute("insert into repeatedFieldUpdatable values ('gg', 'hh'), ('jj', 'll')");

    PreparedStatement preparedStatement =
        sharedConn.prepareStatement(
            "SELECT t1, t2, t1 as t3 FROM repeatedFieldUpdatable",
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_UPDATABLE);
    ResultSet rs = preparedStatement.executeQuery();
    while (rs.next()) {
      rs.getObject(3);
    }
  }

  @Test
  public void updatableDefaultPrimaryField() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 2, 0));
    Statement stmt = sharedConn.createStatement();
    stmt.executeQuery("DROP TABLE IF EXISTS `testDefaultUUID`");
    stmt.execute(
        "CREATE TABLE `testDefaultUUID` ("
            + "`column1` varchar(40) NOT NULL DEFAULT uuid(),"
            + "`column2` varchar(100) DEFAULT NULL,"
            + " PRIMARY KEY (`column1`))");
    String sql = "SELECT t.* FROM testDefaultUUID t WHERE 1 = 2";
    try (PreparedStatement pstmt =
        sharedConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      pstmt.execute();
      ResultSet rs = pstmt.getResultSet();
      rs.moveToInsertRow();
      rs.updateString("column2", "x");
      try {
        rs.insertRow();
        if (!isMariaDBServer() || !minVersion(10, 5, 3)) {
          fail("Must have thrown exception");
        }
        rs.next();
        assertEquals(36, rs.getString(1).length());
        assertEquals("x", rs.getString(2));
      } catch (SQLException e) {
        if (isMariaDBServer() && minVersion(10, 5, 3)) {
          fail("Must have succeed");
        }
        assertTrue(
            e.getMessage()
                .contains(
                    "Cannot call insertRow() not setting value for primary key column1 with "
                        + "default value before server 10.5"));
      }
      rs.moveToInsertRow();
      rs.updateString("column1", "de6f7774-e399-11ea-aa68-c8348e0fed44");
      rs.updateString("column2", "x");
      rs.insertRow();
      rs.next();
      assertEquals("de6f7774-e399-11ea-aa68-c8348e0fed44", rs.getString(1));
      assertEquals("x", rs.getString(2));
    }
  }
}
