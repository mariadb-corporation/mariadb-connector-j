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
import org.mariadb.jdbc.type.*;

public class GeometryCollectionCodecTest extends CommonCodecTest {
  public static org.mariadb.jdbc.Connection geoConn;

  private Polygon poly1 =
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

  GeometryCollection geo1 =
      new GeometryCollection(
          new Geometry[] {
            new Point(0, 0),
            new LineString(
                new Point[] {
                  new Point(10, 10),
                  new Point(20, 10),
                  new Point(20, 20),
                  new Point(10, 20),
                  new Point(10, 10)
                },
                true)
          });
  GeometryCollection geo2 =
      new GeometryCollection(
          new Geometry[] {
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
                }),
            new MultiPoint(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)})
          });
  GeometryCollection geo3 =
      new GeometryCollection(
          new Geometry[] {
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
                })
          });

  @BeforeAll
  public static void beforeAll2() throws Exception {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        "CREATE TABLE GeometryCollectionCodec (t1 GeometryCollection, t2 GeometryCollection, t3 GeometryCollection, t4 GeometryCollection)");
    stmt.execute(
        "INSERT INTO GeometryCollectionCodec VALUES "
            + "(ST_GeomFromText('GeometryCollection(POINT (0 0), LINESTRING(10 10,20 10,20 20,10 20,10 10))'), "
            + "ST_GeomFromText('GeometryCollection(POLYGON((0 0,50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10)), MULTIPOINT(0 0,0 10,10 0))'), "
            + "ST_GeomFromText('GeometryCollection(MULTILINESTRING((0 0,50 0,50 50,0 50), (10 10,20 10,20 20,10 20)), MULTIPOLYGON(((1 1, 1 8,4 9,6 9,9 3,7 2, 1 1))))'), "
            + "null)");
    stmt.execute(
        "CREATE TABLE GeometryCollectionCodec2 (id int not null primary key auto_increment, t1 GeometryCollection)");
    stmt.execute("FLUSH TABLES");
    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (org.mariadb.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS GeometryCollectionCodec");
    stmt.execute("DROP TABLE IF EXISTS GeometryCollectionCodec2");
    if (geoConn != null) geoConn.close();
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from GeometryCollectionCodec");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(org.mariadb.jdbc.Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from GeometryCollectionCodec"
                + " WHERE 1 > ?");
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
      assertEquals(geo1, rs.getObject(1));
      assertFalse(rs.wasNull());
      assertEquals(geo2, rs.getObject(2));
      assertFalse(rs.wasNull());
      assertEquals(geo3, rs.getObject(3));
      assertFalse(rs.wasNull());
      assertNull(rs.getObject(4));
      assertTrue(rs.wasNull());
    } else {
      assertEquals(geo1, rs.getObject(1, GeometryCollection.class));
      assertFalse(rs.wasNull());
      assertEquals(geo2, rs.getObject(2, GeometryCollection.class));
      assertFalse(rs.wasNull());
      assertEquals(geo3, rs.getObject(3, GeometryCollection.class));
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
    testErrObject(rs, LineString.class);
    testErrObject(rs, Point.class);
    testErrObject(rs, Polygon.class);
    testErrObject(rs, MultiLineString.class);
    testErrObject(rs, MultiPoint.class);
    testErrObject(rs, MultiPolygon.class);
    testErrObject(rs, Double.class);
    testErrObject(rs, Float.class);
    testErrObject(rs, Byte.class);

    String hexa =
        "000000000107000000020000000101000000000000000000000000000000000000000102000000050000000000000000002440000000000000244000000000000034400000000000002440000000000000344000000000000034400000000000002440000000000000344000000000000024400000000000002440";
    testArrObject(rs, byte[].class, MultiPolygonCodecTest.decodeHexString(hexa));

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
      assertEquals("GEOMETRYCOLLECTION", meta.getColumnTypeName(1));
    } else {
      assertEquals("GEOMETRY", meta.getColumnTypeName(1));
    }
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));
    assertEquals(
        geoDefault ? GeometryCollection.class.getName() : byte[].class.getName(),
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
    stmt.execute("TRUNCATE TABLE GeometryCollectionCodec2");

    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO GeometryCollectionCodec2(t1) VALUES (?)")) {
      prep.setObject(1, geo1);
      prep.execute();
      prep.setObject(1, (GeometryCollection) null);
      prep.execute();

      prep.setObject(1, geo2);
      prep.addBatch();
      prep.setObject(1, geo3);
      prep.addBatch();
      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM GeometryCollectionCodec2");
    assertTrue(rs.next());
    assertEquals(geo1, rs.getObject(2, GeometryCollection.class));
    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, GeometryCollection.class));

    assertTrue(rs.next());
    assertNull(rs.getObject(2, GeometryCollection.class));
    rs.updateObject(2, geo2);
    rs.updateRow();
    assertEquals(geo2, rs.getObject(2, GeometryCollection.class));
    assertTrue(rs.next());

    assertEquals(geo2, rs.getObject(2, GeometryCollection.class));
    assertTrue(rs.next());
    assertEquals(geo3, rs.getObject(2, GeometryCollection.class));
  }
}
