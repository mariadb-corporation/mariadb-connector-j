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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.result.CompleteResult;
import org.mariadb.jdbc.type.*;

public class MultiPolygonCodecTest extends CommonCodecTest {
  public static org.mariadb.jdbc.Connection geoConn;
  private final MultiPolygon ls1 =
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
  private final MultiPolygon ls2 =
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

  private final MultiPolygon ls3 =
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
    // xpand doesn't recognized MultiPolygon
    Assumptions.assumeFalse(isXpand());

    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE MultiPolygonCodec (t1 MultiPolygon, t2 MultiPolygon, t3 MultiPolygon, t4 MultiPolygon)");
    stmt.execute(
        "INSERT INTO MultiPolygonCodec VALUES "
            + "(ST_MPolyFromText('MULTIPOLYGON(((1 1, 1 5,4 9,6 9,9 3,7 2, 1 1)), ((0 0, 50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10)))'), "
            + "ST_MPolyFromText('MULTIPOLYGON(((1 1, 1 8,4 9,6 9,9 3,7 2, 1 1)))'), "
            + "ST_MPolyFromText('MULTIPOLYGON(((0 0, 50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10)))'), null)");
    stmt.execute(
        "CREATE TABLE MultiPolygonCodec2 (id int not null primary key auto_increment, t1 MultiPolygon)");
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
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from MultiPolygonCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private CompleteResult getPrepare(org.mariadb.jdbc.Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from MultiPolygonCodec"
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
    testArrObject(rs, decodeHexString(hexa));

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
    stmt.execute("TRUNCATE TABLE MultiPolygonCodec2");
    stmt.execute("START TRANSACTION"); // if MAXSCALE ensure using WRITER
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO MultiPolygonCodec2(t1) VALUES (?)")) {
      prep.setObject(1, ls1);
      prep.execute();
      prep.setObject(1, (MultiPolygon) null);
      prep.execute();

      prep.setObject(1, ls2);
      prep.addBatch();
      prep.setObject(1, ls1);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM MultiPolygonCodec2");
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
    con.commit();
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
