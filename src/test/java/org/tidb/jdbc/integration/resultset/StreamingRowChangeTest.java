// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.integration.resultset;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.tidb.jdbc.Statement;
import org.tidb.jdbc.integration.Common;

public class StreamingRowChangeTest extends Common {

  private final String NOT_FORWARD = "Operation not permit on TYPE_FORWARD_ONLY resultSet";
  private final Class<? extends java.lang.Exception> sqle = SQLException.class;

  @AfterAll
  public static void after2() throws SQLException {
    sharedConn.createStatement().execute("DROP TABLE ResultSetTest");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS ResultSetTest");
    stmt.execute("CREATE TABLE ResultSetTest (t1 int not null primary key auto_increment, t2 int)");
    stmt.execute("INSERT INTO ResultSetTest(t2) values (1),(2),(3),(4),(5),(6),(7),(8)");
  }

  @Test
  public void closedRes() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    rs.close();
    String NOT_SUPPORTED = "Operation not permit on a closed resultSet";
    Common.assertThrowsContains(sqle, rs::next, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::isBeforeFirst, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::isAfterLast, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::isFirst, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::isLast, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::beforeFirst, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::afterLast, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::first, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::last, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::getRow, NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, () -> rs.absolute(0), NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, () -> rs.relative(1), NOT_SUPPORTED);
    Common.assertThrowsContains(sqle, rs::previous, NOT_SUPPORTED);
  }

  @Test
  public void next() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    // next fetching no result
    stmt.setFetchSize(8);
    rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());

    // next fetching no result keeping all results
    stmt = sharedConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(8);
    rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());
  }

  @Test
  public void isAfterLast() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    isAfterLast(stmt);
    stmt.setFetchSize(3);
    isAfterLast(stmt);
    stmt.setFetchSize(4);
    isAfterLast(stmt);
  }

  public void isAfterLast(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertFalse(rs.isAfterLast());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.isAfterLast());
    assertFalse(rs.next());
    assertTrue(rs.isAfterLast());

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest WHERE t1 < 0");
    assertFalse(rs.isAfterLast());
    assertFalse(rs.next());
    assertFalse(rs.isAfterLast());
  }

  @Test
  public void isFirst() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    Statement stmt2 =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    isFirst(stmt, stmt2);

    stmt.setFetchSize(3);
    stmt2.setFetchSize(3);
    isFirst(stmt, stmt2);

    stmt.setFetchSize(4);
    stmt2.setFetchSize(4);
    isFirst(stmt, stmt2);
  }

  private void isFirst(Statement stmt, Statement stmt2) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertFalse(rs.isFirst());
    assertTrue(rs.next());
    assertTrue(rs.isFirst());
    while (rs.next()) {
      assertFalse(rs.isFirst());
    }
    assertFalse(rs.isFirst());

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest Where 1 = -1");
    assertFalse(rs.isFirst());
    assertFalse(rs.next());
    assertFalse(rs.isFirst());

    rs = stmt2.executeQuery("SELECT * FROM ResultSetTest");
    assertFalse(rs.isFirst());
    assertTrue(rs.next());
    assertTrue(rs.isFirst());
    while (rs.next()) {
      assertFalse(rs.isFirst());
    }
    assertFalse(rs.isFirst());

    rs = stmt2.executeQuery("SELECT * FROM ResultSetTest Where 1 = -1");
    assertFalse(rs.isFirst());
    assertFalse(rs.next());
    assertFalse(rs.isFirst());
  }

  @Test
  public void isLast() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    isLast(stmt);
    stmt.setFetchSize(3);
    isLast(stmt);
    stmt.setFetchSize(4);
    isLast(stmt);
  }

  private void isLast(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertFalse(rs.isLast());
    for (int i = 1; i < 8; i++) {
      assertTrue(rs.next());
      assertFalse(rs.isLast());
      assertEquals(i, rs.getInt(1));
    }
    assertTrue(rs.next());
    assertEquals(8, rs.getInt(1));
    assertTrue(rs.isLast());
    assertFalse(rs.next());
    assertFalse(rs.isLast());

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest Where 1 = -1");
    rs.isLast();
    assertFalse(rs.isLast());
    assertFalse(rs.next());
    assertFalse(rs.isLast());

    stmt = sharedConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(3);
    rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertFalse(rs.isLast());
    for (int i = 1; i < 8; i++) {
      assertTrue(rs.next());
      assertFalse(rs.isLast());
      assertEquals(i, rs.getInt(1));
    }
    assertTrue(rs.next());
    assertEquals(8, rs.getInt(1));
    assertTrue(rs.isLast());
    assertFalse(rs.next());
    assertFalse(rs.isLast());

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest Where 1 = -1");
    assertFalse(rs.isLast());
    assertFalse(rs.next());
    assertFalse(rs.isLast());
  }

  @Test
  public void isBeforeFirst() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    isBeforeFirst(stmt);
    stmt.setFetchSize(3);
    isBeforeFirst(stmt);
  }

  private void isBeforeFirst(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertTrue(rs.isBeforeFirst());
    while (rs.next()) {
      assertFalse(rs.isBeforeFirst());
    }
    assertFalse(rs.isBeforeFirst());

    stmt = sharedConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(3);
    rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertTrue(rs.isBeforeFirst());
    while (rs.next()) {
      assertFalse(rs.isBeforeFirst());
    }
    assertFalse(rs.isBeforeFirst());
  }

  @Test
  public void beforeFirst() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    beforeFirst(stmt);
    stmt.setFetchSize(3);
    beforeFirst(stmt);
  }

  private void beforeFirst(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());
    assertFalse(rs.isBeforeFirst());
    rs.beforeFirst();
    assertTrue(rs.isBeforeFirst());
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs2 = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(sqle, rs2::beforeFirst, NOT_FORWARD);
  }

  @Test
  public void afterLast() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    afterLast(stmt);
    stmt.setFetchSize(3);
    afterLast(stmt);
  }

  private void afterLast(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    rs.afterLast();
    assertFalse(rs.next());
    assertTrue(rs.previous());
    assertEquals(8, rs.getInt(1));

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs2 = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(sqle, rs2::afterLast, NOT_FORWARD);
  }

  @Test
  public void first() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    first(stmt);
    stmt.setFetchSize(3);
    first(stmt);
  }

  private void first(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    rs.afterLast();
    assertTrue(rs.first());
    assertEquals(1, rs.getInt(1));

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest WHERE t1 = -1");
    assertFalse(rs.first());

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs2 = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(sqle, rs2::first, NOT_FORWARD);
  }

  @Test
  public void last() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    last(stmt);
    stmt.setFetchSize(3);
    last(stmt);
  }

  private void last(Statement stmt) throws SQLException {
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertTrue(rs.last());
    assertEquals(8, rs.getInt(1));

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest WHERE t1 = -1");
    assertFalse(rs.last());

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertTrue(rs.last());
    assertEquals(8, rs.getInt(1));

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest WHERE t1 = -1");
    assertFalse(rs.last());
  }

  @Test
  public void getRow() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertEquals(0, rs.getRow());
    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(0, rs.getRow());
    }
    assertFalse(rs.next());
    assertEquals(0, rs.getRow());

    rs = stmt.executeQuery("SELECT * FROM ResultSetTest WHERE t1 = -1");
    assertEquals(0, rs.getRow());
    assertFalse(rs.next());
    assertEquals(0, rs.getRow());
  }

  @Test
  public void absolute() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertFalse(rs.absolute(0));
    assertTrue(rs.absolute(2));
    assertEquals(2, rs.getRow());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.absolute(6));
    assertEquals(6, rs.getRow());
    assertEquals(6, rs.getInt(1));
    assertFalse(rs.absolute(9));
    assertTrue(rs.absolute(4));
    assertEquals(4, rs.getRow());
    assertEquals(4, rs.getInt(1));
    assertTrue(rs.absolute(5));
    assertEquals(5, rs.getRow());
    assertEquals(5, rs.getInt(1));
    assertFalse(rs.absolute(9));
    assertTrue(rs.isAfterLast());
    assertTrue(rs.absolute(1));
    assertTrue(rs.isFirst());

    assertFalse(rs.absolute(-9));
    assertFalse(rs.isAfterLast());
    assertTrue(rs.absolute(-8));
    assertTrue(rs.isFirst());
    assertTrue(rs.absolute(-3));
    assertEquals(6, rs.getInt(1));

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs2 = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(sqle, () -> rs2.absolute(0), NOT_FORWARD);
  }

  @Test
  public void relative() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    rs.relative(2);
    assertEquals(2, rs.getInt(1));
    rs.relative(2);
    assertEquals(4, rs.getInt(1));
    rs.relative(10);
    assertTrue(rs.isAfterLast());
    rs.relative(-20);
    assertTrue(rs.isBeforeFirst());
    rs.relative(5);
    assertEquals(5, rs.getInt(1));

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs2 = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(sqle, () -> rs2.relative(0), NOT_FORWARD);
  }

  @Test
  public void previous() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.setFetchSize(3);
    ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertFalse(rs.previous());
    rs.afterLast();
    for (int i = 8; i > 0; i--) {
      assertTrue(rs.previous());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.previous());

    stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs2 = stmt.executeQuery("SELECT * FROM ResultSetTest");
    Common.assertThrowsContains(sqle, rs2::previous, NOT_FORWARD);
  }

  @Test
  public void getFetchSize() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertEquals(3, rs.getFetchSize());

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));

    rs.setFetchSize(2);
    assertEquals(2, rs.getFetchSize());

    for (int i = 3; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());
    Common.assertThrowsContains(sqle, () -> rs.setFetchSize(-2), "invalid fetch size -2");
  }

  @Test
  public void removeStreaming() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.setFetchSize(3);
    final ResultSet rs = stmt.executeQuery("SELECT * FROM ResultSetTest");
    assertEquals(3, rs.getFetchSize());
    rs.setFetchSize(0);
    assertEquals(0, rs.getFetchSize());

    for (int i = 1; i < 9; i++) {
      assertTrue(rs.next());
      assertEquals(i, rs.getInt(1));
    }
    assertFalse(rs.next());
  }
}
