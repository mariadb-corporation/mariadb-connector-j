// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.type.GeometryCollection;
import com.singlestore.jdbc.type.Point;
import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PointCodecTest extends CommonCodecTest {
  public static com.singlestore.jdbc.Connection geoConn;

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS PointCodec");
    stmt.execute("DROP TABLE IF EXISTS PointCodec2");
    if (geoConn != null) geoConn.close();
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE PointCodec (t1 POINT, t2 POINT, t3 POINT, t4 POINT, id INT)");
    stmt.execute(
        "INSERT INTO PointCodec VALUES "
            + "(ST_PointFromText('POINT(10 1)'), ST_PointFromText('POINT(1.5 18)'), ST_PointFromText('POINT(-1 0.55)'), null, 1)");
    stmt.execute(
        createRowstore()
            + " TABLE PointCodec2 (id int not null primary key auto_increment, t1 POINT)");
    stmt.execute("FLUSH TABLES");

    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (com.singlestore.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from PointCodec ORDER BY id");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(com.singlestore.jdbc.Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from PointCodec"
                + " WHERE 1 > ? ORDER BY id");
    stmt.closeOnCompletion();
    stmt.setInt(1, 0);
    CompleteResult rs = (CompleteResult) stmt.executeQuery();
    assertTrue(rs.next());
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(get(), false);
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(getPrepare(sharedConn), false);
    getObject(getPrepare(sharedConnBinary), false);
    getObject(getPrepare(geoConn), true);
  }

  public void getObject(ResultSet rs, boolean defaultGeo) throws SQLException {
    if (defaultGeo
        && isMariaDBServer()
        && minVersion(10, 5, 1)
        && !"maxscale".equals(System.getenv("srv"))
        && !"skysql-ha".equals(System.getenv("srv"))) {
      assertEquals(new Point(10, 1), rs.getObject(1));
      assertFalse(rs.wasNull());
      assertEquals(new Point(1.5, 18), rs.getObject(2));
      assertFalse(rs.wasNull());
      assertEquals(new Point(-1, 0.55), rs.getObject(3));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4));
      assertTrue(rs.wasNull());
    } else {
      assertEquals(new Point(10, 1), rs.getObject(1, Point.class));
      assertFalse(rs.wasNull());
      assertEquals(new Point(1.5, 18), rs.getObject(2, Point.class));
      assertFalse(rs.wasNull());
      assertEquals(new Point(-1, 0.55), rs.getObject(3, Point.class));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4, Point.class));
      assertTrue(rs.wasNull());
    }
  }

  @Test
  public void getObjectType() throws Exception {
    getObjectType(get());
  }

  @Test
  public void getObjectTypePrepare() throws Exception {
    getObjectType(getPrepare(sharedConn));
    getObjectType(getPrepare(sharedConnBinary));
  }

  public void getObjectType(ResultSet rs) throws Exception {
    testErrObject(rs, Integer.class);
    testErrObject(rs, String.class);
    testErrObject(rs, Long.class);
    testErrObject(rs, Short.class);
    testErrObject(rs, BigDecimal.class);
    testErrObject(rs, BigInteger.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);
    testArrObject(
        rs,
        byte[].class,
        new byte[] {
          (byte) 0x00,
          0x00,
          0x00,
          0x00,
          0x01,
          0x01,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x24,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0xF0,
          0x3F
        });
    testErrObject(rs, Boolean.class);
    testErrObject(rs, Clob.class);
    testErrObject(rs, NClob.class);
    testErrObject(rs, InputStream.class);
    testErrObject(rs, Reader.class);
    testErrObject(rs, java.util.Date.class);
  }

  @Test
  public void getMetaData() throws SQLException {
    getMetaData(sharedConn, false);
    try (com.singlestore.jdbc.Connection con = createCon("geometryDefaultType=default")) {
      getMetaData(con, true);
    }
  }

  private void getMetaData(com.singlestore.jdbc.Connection con, boolean geoDefault)
      throws SQLException {
    ResultSet rs = getPrepare(con);
    ResultSetMetaData meta = rs.getMetaData();
    if (isMariaDBServer()
        && minVersion(10, 5, 1)
        && !"maxscale".equals(System.getenv("srv"))
        && !"skysql-ha".equals(System.getenv("srv"))) {
      assertEquals("POINT", meta.getColumnTypeName(1));
    } else {
      assertEquals("GEOMETRY", meta.getColumnTypeName(1));
    }
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals(
        geoDefault
            ? ((isMariaDBServer()
                    && minVersion(10, 5, 1)
                    && !"maxscale".equals(System.getenv("srv"))
                    && !"skysql-ha".equals(System.getenv("srv")))
                ? Point.class.getName()
                : GeometryCollection.class.getName())
            : byte[].class.getName(),
        meta.getColumnClassName(1));

    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.VARBINARY, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
  }

  @Test
  public void sendParam() throws Exception {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE PointCodec2");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO PointCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setObject(2, new Point(52.1, 12.8));
      prep.execute();
      prep.setInt(1, 2);
      prep.setObject(2, (Point) null);
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, new Point(2.2, 3.3));
      prep.addBatch();
      prep.setObject(2, new Point(2, 3));
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM PointCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertEquals(new Point(52.1, 12.8), rs.getObject(2, Point.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, Point.class));
    assertTrue(rs.next());
    assertNull(rs.getObject(2, Point.class));
    rs.updateObject(2, new Point(1, 8));
    rs.updateRow();
    assertEquals(new Point(1, 8), rs.getObject(2, Point.class));

    assertTrue(rs.next());
    assertEquals(new Point(2.2, 3.3), rs.getObject(2, Point.class));
    assertTrue(rs.next());
    assertEquals(new Point(2, 3), rs.getObject(2, Point.class));
  }

  @Test
  public void equal() {
    Point pt = new Point(0, 10);
    assertEquals(pt, pt);
    assertEquals(new Point(0, 10), pt);
    assertEquals(new Point(0, 10).hashCode(), pt.hashCode());
    assertFalse(pt.equals(null));
    assertFalse(pt.equals(""));
    assertNotEquals(new Point(0, 20), pt);
    assertNotEquals(new Point(10, 10), pt);
  }
}
