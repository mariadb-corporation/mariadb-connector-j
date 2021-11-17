// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.type.GeometryCollection;
import org.mariadb.jdbc.type.LineString;
import org.mariadb.jdbc.type.Point;

public class LineStringCodecTest extends CommonCodecTest {
  public static org.mariadb.jdbc.Connection geoConn;

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS LineStringCodec");
    stmt.execute("DROP TABLE IF EXISTS LineStringCodec2");
    if (geoConn != null) geoConn.close();
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE LineStringCodec (t1 LineString, t2 LineString, t3 LineString, t4 LineString)");
    stmt.execute(
        "INSERT INTO LineStringCodec VALUES "
            + "( ST_LineStringFromText('LINESTRING(0 0,0 10,10 0)'), ST_LineStringFromText('LINESTRING(10 10,20 10,20 20,10 20,10 10)'), ST_LineStringFromText('LINESTRING(-1 0.55, 3 5, 1 1)'), null)");
    stmt.execute(
        "CREATE TABLE LineStringCodec2 (id int not null primary key auto_increment, t1 LineString)");
    stmt.execute("FLUSH TABLES");
    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (org.mariadb.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from LineStringCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private CompleteResult getPrepare(org.mariadb.jdbc.Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from LineStringCodec"
                + " WHERE 1 > ?");
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    CompleteResult rs = (CompleteResult) preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
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
      assertEquals(
          new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true),
          rs.getObject(1));
      assertFalse(rs.wasNull());
      assertEquals(
          new LineString(
              new Point[] {
                new Point(10, 10),
                new Point(20, 10),
                new Point(20, 20),
                new Point(10, 20),
                new Point(10, 10)
              },
              true),
          rs.getObject(2));
      assertFalse(rs.wasNull());
      assertEquals(
          new LineString(new Point[] {new Point(-1, 0.55), new Point(3, 5), new Point(1, 1)}, true),
          rs.getObject(3));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4));
      assertTrue(rs.wasNull());
    } else {
      assertEquals(
          new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true),
          rs.getObject(1, LineString.class));
      assertFalse(rs.wasNull());
      assertEquals(
          new LineString(
              new Point[] {
                new Point(10, 10),
                new Point(20, 10),
                new Point(20, 20),
                new Point(10, 20),
                new Point(10, 10)
              },
              true),
          rs.getObject(2, LineString.class));
      assertFalse(rs.wasNull());
      assertEquals(
          new LineString(new Point[] {new Point(-1, 0.55), new Point(3, 5), new Point(1, 1)}, true),
          rs.getObject(3, LineString.class));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4));
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
    testErrObject(rs, GeometryCollection.class);
    testErrObject(rs, Byte.class);
    testArrObject(
        rs,
        new byte[] {
          (byte) 0x00,
          0x00,
          0x00,
          0x00,
          0x01,
          0x02,
          0x00,
          0x00,
          0x00,
          0x03,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
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
          0x24,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00
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
    try (org.mariadb.jdbc.Connection con = createCon("geometryDefaultType=default")) {
      getMetaData(con, true);
    }
  }

  private void getMetaData(org.mariadb.jdbc.Connection con, boolean geoDefault)
      throws SQLException {
    ResultSet rs = getPrepare(con);
    ResultSetMetaData meta = rs.getMetaData();
    if (isMariaDBServer()
        && minVersion(10, 5, 1)
        && !"maxscale".equals(System.getenv("srv"))
        && !"skysql-ha".equals(System.getenv("srv"))) {
      assertEquals("LINESTRING", meta.getColumnTypeName(1));
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
                ? LineString.class.getName()
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
    stmt.execute("TRUNCATE TABLE LineStringCodec2");
    LineString ls1 =
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true);
    LineString ls2 =
        new LineString(
            new Point[] {
              new Point(10, 10),
              new Point(20, 10),
              new Point(20, 20),
              new Point(10, 20),
              new Point(10, 10)
            },
            true);
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO LineStringCodec2(t1) VALUES (?)")) {
      prep.setObject(1, ls1);
      prep.execute();
      prep.setObject(1, (LineString) null);
      prep.execute();

      prep.setObject(1, ls2);
      prep.addBatch();
      prep.setObject(1, ls1);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM LineStringCodec2");
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
    LineString ls =
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true);
    assertTrue(ls.isOpen());
    assertEquals(ls, ls);
    assertEquals(
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true),
        ls);
    assertEquals(
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true)
            .hashCode(),
        ls.hashCode());
    assertFalse(ls.equals(null));
    assertFalse(ls.equals(""));
    assertNotEquals(
        new LineString(new Point[] {new Point(0, 0), new Point(0, 20), new Point(20, 0)}, true),
        ls);
    assertNotEquals(
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, false),
        ls);
    assertNotEquals(
        new LineString(
            new Point[] {new Point(0, 0), new Point(0, 20), new Point(20, 10), new Point(10, 0)},
            true),
        ls);
  }
}
