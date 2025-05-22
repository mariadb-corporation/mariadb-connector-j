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
import org.mariadb.jdbc.type.MultiLineString;
import org.mariadb.jdbc.type.Point;
import org.mariadb.jdbc.util.constants.Capabilities;

public class MultiLineStringCodecTest extends CommonCodecTest {
  public static org.mariadb.jdbc.Connection geoConn;
  private final MultiLineString ls1 =
      new MultiLineString(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(1, 1),
                  new Point(1, 5),
                  new Point(4, 9),
                  new Point(6, 9),
                  new Point(9, 3),
                  new Point(7, 2)
                },
                true)
          });
  private final MultiLineString ls2 =
      new MultiLineString(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(0, 0), new Point(50, 0), new Point(50, 50), new Point(0, 50)
                },
                true),
            new LineString(
                new Point[] {
                  new Point(10, 10), new Point(20, 10), new Point(20, 20), new Point(10, 20)
                },
                true)
          });

  private final MultiLineString ls3 =
      new MultiLineString(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(0, 0), new Point(50, 0), new Point(50, 50), new Point(0, 50)
                },
                true)
          });

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS MultiLineStringCodec");
    stmt.execute("DROP TABLE IF EXISTS MultiLineStringCodec2");
    if (geoConn != null) geoConn.close();
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    // xpand doesn't recognized MultiLineString
    Assumptions.assumeFalse(isXpand());
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE MultiLineStringCodec (t1 MultiLineString, t2 MultiLineString, t3"
            + " MultiLineString, t4 MultiLineString)");
    stmt.execute(
        "INSERT INTO MultiLineStringCodec VALUES (ST_MLineFromText('MULTILINESTRING((1 1,1 5,4 9,6"
            + " 9,9 3,7 2))'), ST_MLineFromText('MULTILINESTRING((0 0,50 0,50 50,0 50), (10 10,20"
            + " 10,20 20,10 20))'), ST_MLineFromText('MULTILINESTRING((0 0,50 0,50 50,0 50))'),"
            + " null)");
    stmt.execute(
        "CREATE TABLE MultiLineStringCodec2 (id int not null primary key auto_increment, t1"
            + " MultiLineString)");
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
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from"
                + " MultiLineStringCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private CompleteResult getPrepare(org.mariadb.jdbc.Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from"
                + " MultiLineStringCodec WHERE 1 > ?",
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
      assertEquals(ls1, rs.getObject(1, MultiLineString.class));
      assertFalse(rs.wasNull());
      // MultiLineString((0 0,50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10))
      assertEquals(ls2, rs.getObject(2, MultiLineString.class));
      assertFalse(rs.wasNull());
      assertEquals(ls3, rs.getObject(3, MultiLineString.class));
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
          0x05,
          0x00,
          0x00,
          0x00,
          0x01,
          0x00,
          0x00,
          0x00,
          0x01,
          0x02,
          0x00,
          0x00,
          0x00,
          0x06,
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
          0x40
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
    if (hasCapability(Capabilities.EXTENDED_METADATA)) {
      assertEquals("MULTILINESTRING", meta.getColumnTypeName(1));
    } else {
      assertEquals("GEOMETRY", meta.getColumnTypeName(1));
    }
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals(
        geoDefault
            ? (hasCapability(Capabilities.EXTENDED_METADATA)
                ? MultiLineString.class.getName()
                : GeometryCollection.class.getName())
            : "byte[]",
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
    stmt.execute("TRUNCATE TABLE MultiLineStringCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO MultiLineStringCodec2(t1) VALUES (?)")) {
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
            .executeQuery("SELECT * FROM MultiLineStringCodec2");
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, MultiLineString.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, MultiLineString.class));

    assertTrue(rs.next());
    assertNull(rs.getObject(2, MultiLineString.class));
    rs.updateObject(2, ls2);
    rs.updateRow();
    assertEquals(ls2, rs.getObject(2, MultiLineString.class));
    assertTrue(rs.next());

    assertEquals(ls2, rs.getObject(2, MultiLineString.class));
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, MultiLineString.class));
    con.commit();
  }

  @Test
  public void equal() {
    assertEquals(ls2, ls2);
    assertEquals(
        new MultiLineString(
            new LineString[] {
              new LineString(
                  new Point[] {
                    new Point(0, 0), new Point(50, 0), new Point(50, 50), new Point(0, 50)
                  },
                  true),
              new LineString(
                  new Point[] {
                    new Point(10, 10), new Point(20, 10), new Point(20, 20), new Point(10, 20)
                  },
                  true)
            }),
        ls2);
    assertEquals(
        new MultiLineString(
                new LineString[] {
                  new LineString(
                      new Point[] {
                        new Point(0, 0), new Point(50, 0), new Point(50, 50), new Point(0, 50)
                      },
                      true),
                  new LineString(
                      new Point[] {
                        new Point(10, 10), new Point(20, 10), new Point(20, 20), new Point(10, 20)
                      },
                      true)
                })
            .hashCode(),
        ls2.hashCode());
    assertNotEquals(null, ls2);
    assertNotEquals("", ls2);
    assertNotEquals(ls1, ls2);
    assertNotEquals(
        new MultiLineString(
            new LineString[] {
              new LineString(
                  new Point[] {
                    new Point(0, 0), new Point(50, 0), new Point(50, 50), new Point(0, 50)
                  },
                  true),
              new LineString(
                  new Point[] {
                    new Point(10, 10), new Point(20, 10), new Point(20, 20), new Point(10, 20)
                  },
                  false)
            }),
        ls1);
  }
}
