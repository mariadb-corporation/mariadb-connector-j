// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.type.LineString;
import com.singlestore.jdbc.type.Point;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LineStringCodecTest extends CommonCodecTest {
  public static com.singlestore.jdbc.Connection geoConn;

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS LineStringCodec");
    stmt.execute("DROP TABLE IF EXISTS LineStringCodec2");
    if (geoConn != null) geoConn.close();
  }

  private static void assertEqualLineString(LineString expected, LineString actual) {
    Point[] expectedPoints = expected.getPoints();
    Point[] actualPoints = actual.getPoints();
    for (int i = 0; i < expectedPoints.length; i++) {
      assertEqualCoordinate(expectedPoints[i].getX(), actualPoints[i].getX());
      assertEqualCoordinate(expectedPoints[i].getY(), actualPoints[i].getY());
    }
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        createRowstore()
            + " TABLE LineStringCodec (t1 Geography, t2 Geography, t3 Geography, t4 Geography, id INT)");
    stmt.execute(
        "INSERT INTO LineStringCodec VALUES "
            + "('LINESTRING(0 0,0 10,10 0)', 'LINESTRING(10 10,20 10,20 20,10 20,10 10)', 'LINESTRING(-1 0.55, 3 5, 1 1)', null, 1)");
    stmt.execute(
        createRowstore()
            + " TABLE LineStringCodec2 (id int not null primary key auto_increment, t1 Geography)");
    stmt.execute("FLUSH TABLES");
    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (com.singlestore.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from LineStringCodec ORDER BY id");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(com.singlestore.jdbc.Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from LineStringCodec"
                + " WHERE 1 > ? ORDER BY id");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    CompleteResult rs = (CompleteResult) stmt.executeQuery();
    assertTrue(rs.next());
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(get());
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(getPrepare(sharedConn));
    //    getObject(getPrepare(sharedConnBinary), false);
  }

  public void getObject(ResultSet rs) throws SQLException {
    assertEqualLineString(
        new LineString("LINESTRING(0 0, 0 10, 10 0)"), rs.getObject(1, LineString.class));
    assertFalse(rs.wasNull());
    assertEqualLineString(
        new LineString("LINESTRING(10 10,20 10,20 20,10 20,10 10)"),
        rs.getObject(2, LineString.class));
    assertFalse(rs.wasNull());
    assertEqualLineString(
        new LineString("LINESTRING(-1 0.55, 3 5, 1 1)"), rs.getObject(3, LineString.class));
    assertFalse(rs.wasNull());
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(get());
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepare(sharedConn));
    //    getObjectType(getPrepare(sharedConnBinary));
  }

  public void getObjectType(ResultSet rs) throws Exception {
    testErrObject(rs, Integer.class);
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);
    testObject(
        rs,
        String.class,
        "LINESTRING(0.00000000 0.00000000, 0.00000000 10.00000000, 10.00000000 0.00000000)");
    testObject(rs, Boolean.class, true);
    testErrObject(rs, java.util.Date.class);
  }

  @Test
  public void getMetaData() throws SQLException {
    getMetaData(sharedConn, false);
    //    try (com.singlestore.jdbc.Connection con = createCon("geometryDefaultType=default")) {
    //      getMetaData(con, true);
    //    }
  }

  private void getMetaData(com.singlestore.jdbc.Connection con, boolean geoDefault)
      throws SQLException {
    ResultSet rs = getPrepare(con);
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("STRING", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));

    assertEquals(String.class.getName(), meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.CHAR, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
  }

  @Test
  public void sendParam() throws Exception {
    sendParam(sharedConn);
    //    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE LineStringCodec2");
    LineString ls1 = new LineString("LINESTRING(0 0,0 10,10 0)");
    LineString ls2 = new LineString("LINESTRING(10 10,20 10,20 20,10 20,10 10)");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO LineStringCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setObject(2, ls1);
      prep.execute();
      prep.setInt(1, 2);
      prep.setObject(2, null);
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, ls2);
      prep.addBatch();
      prep.setInt(1, 4);
      prep.setObject(2, ls1);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM LineStringCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, LineString.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, LineString.class));

    assertTrue(rs.next());
    assertNull(rs.getObject(2, LineString.class));
    rs.updateObject(2, ls2);
    rs.updateRow();
    assertEquals(ls2, rs.getObject(2, LineString.class));
    assertTrue(rs.next());

    assertEquals(ls2, rs.getObject(2, LineString.class));
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, LineString.class));
  }

  @Test
  public void equal() {
    LineString ls = new LineString("LINESTRING(0 0,0 10,10 0)");
    assertEquals(ls, ls);
    assertEquals(new LineString("LINESTRING(0 0,0 10,10 0)"), ls);
    assertEquals(new LineString("LINESTRING(0 0,0 10,10 0)").hashCode(), ls.hashCode());
    assertNotEquals(null, ls);
    assertNotEquals("", ls);
    assertNotEquals(new LineString("LINESTRING(0 0,0 20,10 0)"), ls);
    assertNotEquals(new LineString("LINESTRING(0 0,0 10,10 0, 20 0)"), ls);
  }
}
