// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.client.result.CompleteResult;
import com.singlestore.jdbc.type.*;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MultiPolygonCodecTest extends CommonCodecTest {
  public static com.singlestore.jdbc.Connection geoConn;
  private MultiPolygon ls1 =
      new MultiPolygon(
          new Polygon[] {
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
                      false),
                }),
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
                })
          });
  private MultiPolygon ls2 =
      new MultiPolygon(
          new Polygon[] {
            new Polygon(
                new LineString[] {
                  new LineString(
                      new Point[] {
                        new Point(1, 1),
                        new Point(1, 8),
                        new Point(4, 9),
                        new Point(6, 9),
                        new Point(9, 3),
                        new Point(7, 2),
                        new Point(1, 1)
                      },
                      false),
                })
          });

  private MultiPolygon ls3 =
      new MultiPolygon(
          new Polygon[] {
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
                })
          });

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS MultiPolygonCodec");
    stmt.execute("DROP TABLE IF EXISTS MultiPolygonCodec2");
    if (geoConn != null) geoConn.close();
  }

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE MultiPolygonCodec (t1 MultiPolygon, t2 MultiPolygon, t3 MultiPolygon, t4 MultiPolygon, id INT)");
    stmt.execute(
        "INSERT INTO MultiPolygonCodec VALUES "
            + "(ST_MPolyFromText('MULTIPOLYGON(((1 1, 1 5,4 9,6 9,9 3,7 2, 1 1)), ((0 0, 50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10)))'), "
            + "ST_MPolyFromText('MULTIPOLYGON(((1 1, 1 8,4 9,6 9,9 3,7 2, 1 1)))'), "
            + "ST_MPolyFromText('MULTIPOLYGON(((0 0, 50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10)))'), null, 1)");
    stmt.execute(
        "CREATE TABLE MultiPolygonCodec2 (id int not null primary key auto_increment, t1 MultiPolygon)");
    stmt.execute("FLUSH TABLES");

    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (com.singlestore.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from MultiPolygonCodec ORDER BY id");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(com.singlestore.jdbc.Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from MultiPolygonCodec"
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
      assertEquals(ls1, rs.getObject(1));
      assertFalse(rs.wasNull());
      assertEquals(ls2, rs.getObject(2));
      assertFalse(rs.wasNull());
      assertEquals(ls3, rs.getObject(3));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4));
      assertTrue(rs.wasNull());
    } else {
      assertEquals(ls1, rs.getObject(1, MultiPolygon.class));
      assertFalse(rs.wasNull());
      assertEquals(ls2, rs.getObject(2, MultiPolygon.class));
      assertFalse(rs.wasNull());
      assertEquals(ls3, rs.getObject(3, MultiPolygon.class));
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

  private static int toDigit(char hexChar) {
    int digit = Character.digit(hexChar, 16);
    if (digit == -1) {
      throw new IllegalArgumentException("Invalid Hexadecimal Character: " + hexChar);
    }
    return digit;
  }

  public static byte hexToByte(String hexString) {
    int firstDigit = toDigit(hexString.charAt(0));
    int secondDigit = toDigit(hexString.charAt(1));
    return (byte) ((firstDigit << 4) + secondDigit);
  }

  public static byte[] decodeHexString(String hexString) {
    if (hexString.length() % 2 == 1) {
      throw new IllegalArgumentException("Invalid hexadecimal String supplied.");
    }

    byte[] bytes = new byte[hexString.length() / 2];
    for (int i = 0; i < hexString.length(); i += 2) {
      bytes[i / 2] = hexToByte(hexString.substring(i, i + 2));
    }
    return bytes;
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
    String hexa =
        "0000000001060000000200000001030000000100000007000000000000000000F03F000000000000F03F000000000000F03F00000000000014400000000000001040000000000000224000000000000018400000000000002240000000000000224000000000000008400000000000001C400000000000000040000000000000F03F000000000000F03F010300000002000000050000000000000000000000000000000000000000000000000049400000000000000000000000000000494000000000000049400000000000000000000000000000494000000000000000000000000000000000050000000000000000002440000000000000244000000000000034400000000000002440000000000000344000000000000034400000000000002440000000000000344000000000000024400000000000002440";
    testArrObject(rs, byte[].class, decodeHexString(hexa));

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
      assertEquals("MULTIPOLYGON", meta.getColumnTypeName(1));
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
                ? MultiPolygon.class.getName()
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
    stmt.execute("TRUNCATE TABLE MultiPolygonCodec2");

    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO MultiPolygonCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setObject(2, ls1);
      prep.execute();
      prep.setInt(1, 2);
      prep.setObject(2, (MultiPolygon) null);
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, ls2);
      prep.addBatch();
      prep.setObject(2, ls1);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM MultiPolygonCodec2 ORDER BY id");
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, MultiPolygon.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, MultiPolygon.class));

    assertTrue(rs.next());
    assertNull(rs.getObject(2, MultiPolygon.class));
    rs.updateObject(2, ls2);
    rs.updateRow();
    assertEquals(ls2, rs.getObject(2, MultiPolygon.class));
    assertTrue(rs.next());

    assertEquals(ls2, rs.getObject(2, MultiPolygon.class));
    assertTrue(rs.next());
    assertEquals(ls1, rs.getObject(2, MultiPolygon.class));
  }

  @Test
  public void equal() {
    assertEquals(ls1, ls1);
    MultiPolygon testPoly =
        new MultiPolygon(
            new Polygon[] {
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
                        false),
                  }),
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
                  })
            });
    assertEquals(testPoly, ls1);
    assertEquals(testPoly.hashCode(), ls1.hashCode());
    assertFalse(ls1.equals(null));
    assertFalse(ls1.equals(""));
    assertNotEquals(
        new MultiPolygon(
            new Polygon[] {
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
                        false),
                  }),
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
                          new Point(10, 15)
                        },
                        false)
                  })
            }),
        ls1);
  }
}
