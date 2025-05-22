// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.type.GeometryCollection;
import org.mariadb.jdbc.type.LineString;
import org.mariadb.jdbc.type.Point;
import org.mariadb.jdbc.type.Polygon;
import org.mariadb.jdbc.util.constants.Capabilities;

public class PolygonCodecTest extends CommonCodecTest {
  public static org.mariadb.jdbc.Connection geoConn;
  private final Polygon ls1 =
      new Polygon(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(1, 1),
                  new Point(1, 5),
                  new Point(4, 9),
                  new Point(6, 9),
                  new Point(9, 3),
                  new Point(7, 2),
                  new Point(1, 1)
                },
                false)
          });
  private final Polygon ls2 =
      new Polygon(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(0, 0),
                  new Point(50, 0),
                  new Point(50, 50),
                  new Point(0, 50),
                  new Point(0, 0)
                },
                false),
            new LineString(
                new Point[] {
                  new Point(10, 10),
                  new Point(20, 10),
                  new Point(20, 20),
                  new Point(10, 20),
                  new Point(10, 10)
                },
                false)
          });

  private final Polygon ls3 =
      new Polygon(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(0, 0),
                  new Point(50, 0),
                  new Point(50, 50),
                  new Point(0, 50),
                  new Point(0, 0)
                },
                false)
          });

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS PolygonCodec");
    stmt.execute("DROP TABLE IF EXISTS PolygonCodec2");
    if (geoConn != null) geoConn.close();
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    // xpand doesn't recognized Polygon
    Assumptions.assumeFalse(isXpand());
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE PolygonCodec (t1 Polygon, t2 Polygon, t3 Polygon, t4 Polygon)");
    stmt.execute(
        "INSERT INTO PolygonCodec VALUES (ST_PolygonFromText('POLYGON((1 1,1 5,4 9,6 9,9 3,7 2,1"
            + " 1))'), ST_PolygonFromText('POLYGON((0 0,50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10"
            + " 20,10 10))'), ST_PolygonFromText('POLYGON((0 0,50 0,50 50,0 50,0 0))'), null)");
    stmt.execute(
        "CREATE TABLE PolygonCodec2 (id int not null primary key auto_increment, t1 Polygon)");
    stmt.execute("FLUSH TABLES");

    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (org.mariadb.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  private ResultSet get() throws SQLException {
    Statement stmt =
        sharedConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from PolygonCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private CompleteResult getPrepare(org.mariadb.jdbc.Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from PolygonCodec"
                + " WHERE 1 > ?",
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
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
    if (defaultGeo && hasCapability(Capabilities.EXTENDED_METADATA)) {
      assertEquals(ls1, rs.getObject(1));
      assertFalse(rs.wasNull());
      assertEquals(ls2, rs.getObject(2));
      assertFalse(rs.wasNull());
      assertEquals(ls3, rs.getObject(3));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4));
      assertTrue(rs.wasNull());
    } else {
      assertEquals(ls1, rs.getObject(1, Polygon.class));
      assertFalse(rs.wasNull());
      // Polygon((0 0,50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10))
      assertEquals(ls2, rs.getObject(2, Polygon.class));
      assertFalse(rs.wasNull());
      assertEquals(ls3, rs.getObject(3, Polygon.class));
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
    testErrObject(rs, Byte.class);
    testArrObject(
        rs,
        new byte[] {
          (byte) 0x00,
          0x00,
          0x00,
          0x00,
          0x01,
          0x03,
          0x00,
          0x00,
          0x00,
          0x01,
          0x00,
          0x00,
          0x00,
          0x07,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0xF0,
          0x3F,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0xF0,
          0x3F,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0xF0,
          0x3F,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x14,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x10,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x22,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x18,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x22,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x22,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x08,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x1C,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x40,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          0x00,
          (byte) 0xF0,
          0x3F,
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
    getMetaData(sharedConn, false, false);
    try (org.mariadb.jdbc.Connection con = createCon("geometryDefaultType=default")) {
      getMetaData(con, true, false);
    }
    try (org.mariadb.jdbc.Connection con = createCon("useCatalogTerm=Schema")) {
      getMetaData(con, false, true);
    }
  }

  private void getMetaData(org.mariadb.jdbc.Connection con, boolean geoDefault, boolean useSchema)
      throws SQLException {
    ResultSet rs = getPrepare(con);
    ResultSetMetaData meta = rs.getMetaData();
    if (hasCapability(Capabilities.EXTENDED_METADATA)) {
      assertEquals("POLYGON", meta.getColumnTypeName(1));
    } else {
      assertEquals("GEOMETRY", meta.getColumnTypeName(1));
    }
    assertEquals(useSchema ? "def" : database, meta.getCatalogName(1));
    assertEquals(useSchema ? database : "", meta.getSchemaName(1));
    assertEquals(
        (geoDefault
            ? (hasCapability(Capabilities.EXTENDED_METADATA)
                ? Polygon.class.getName()
                : GeometryCollection.class.getName())
            : "byte[]"),
        meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.VARBINARY, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
  }

  @Test
  public void sendParam() throws Exception {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE PolygonCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO PolygonCodec2(t1) VALUES (?)")) {
      prep.setObject(1, ls1);
      prep.execute();
      prep.setObject(1, null);
      prep.execute();

      prep.setObject(1, ls2);
      prep.addBatch();
      prep.setObject(1, ls1);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM PolygonCodec2");
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, Polygon.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, Polygon.class));

    assertTrue(rs.next());
    assertNull(rs.getObject(2, Polygon.class));
    rs.updateObject(2, ls2);
    rs.updateRow();
    assertEquals(ls2, rs.getObject(2, Polygon.class));
    assertTrue(rs.next());

    assertEquals(ls2, rs.getObject(2, Polygon.class));
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, Polygon.class));
    con.commit();
  }

  @Test
  public void equal() {
    assertEquals(ls2, ls2);
    Polygon testPoly =
        new Polygon(
            new LineString[] {
              new LineString(
                  new Point[] {
                    new Point(0, 0),
                    new Point(50, 0),
                    new Point(50, 50),
                    new Point(0, 50),
                    new Point(0, 0)
                  },
                  false),
              new LineString(
                  new Point[] {
                    new Point(10, 10),
                    new Point(20, 10),
                    new Point(20, 20),
                    new Point(10, 20),
                    new Point(10, 10)
                  },
                  false)
            });
    assertEquals(testPoly, ls2);
    assertEquals(testPoly.hashCode(), ls2.hashCode());
    assertNotEquals(null, ls2);
    assertNotEquals("", ls2);
    assertNotEquals(
        new Polygon(
            new LineString[] {
              new LineString(
                  new Point[] {
                    new Point(0, 0),
                    new Point(50, 0),
                    new Point(50, 60),
                    new Point(0, 50),
                    new Point(0, 0)
                  },
                  false),
              new LineString(
                  new Point[] {
                    new Point(10, 10),
                    new Point(20, 10),
                    new Point(20, 20),
                    new Point(10, 20),
                    new Point(10, 10)
                  },
                  false)
            }),
        ls2);
  }
}
