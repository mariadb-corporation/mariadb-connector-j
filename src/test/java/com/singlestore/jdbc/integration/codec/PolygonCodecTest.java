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
import com.singlestore.jdbc.type.Polygon;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PolygonCodecTest extends CommonCodecTest {
  public static com.singlestore.jdbc.Connection geoConn;

  private Polygon ls1 =
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
                })
          });
  private Polygon ls2 =
      new Polygon(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(0, 0),
                  new Point(50, 0),
                  new Point(50, 50),
                  new Point(0, 50),
                  new Point(0, 0)
                }),
            new LineString(
                new Point[] {
                  new Point(10, 10),
                  new Point(20, 10),
                  new Point(20, 20),
                  new Point(10, 20),
                  new Point(10, 10)
                })
          });

  private Polygon ls3 =
      new Polygon(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(0, 0),
                  new Point(50, 0),
                  new Point(50, 50),
                  new Point(0, 50),
                  new Point(0, 0)
                })
          });

  private Polygon ls4 =
      new Polygon(
          new LineString[] {
            new LineString(
                new Point[] {
                  new Point(1, 1),
                  new Point(1, 1),
                  new Point(1, 5),
                  new Point(4, 9),
                  new Point(6, 9),
                  new Point(9, 3),
                  new Point(7, 2),
                  new Point(1, 1),
                  new Point(1, 1)
                })
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
    Statement stmt = sharedConn.createStatement();
    stmt.execute(
        createRowstore()
            + " TABLE PolygonCodec (t1 Geography, t2 Geography, t3 Geography, t4 Geography, id INT)");
    stmt.execute(
        "INSERT INTO PolygonCodec VALUES "
            + "('POLYGON((1 1,1 5,4 9,6 9,9 3,7 2,1 1))', "
            + "'POLYGON((0 0,50 0,50 50,0 50,0 0), (10 10,20 10,20 20,10 20,10 10))', "
            + "'POLYGON((0 0,50 0,50 50,0 50,0 0))', null, 1)");
    stmt.execute(
        createRowstore()
            + " TABLE PolygonCodec2 (id int not null primary key auto_increment, t1 Geography)");
    stmt.execute("FLUSH TABLES");

    String binUrl =
        mDefUrl + (mDefUrl.indexOf("?") > 0 ? "&" : "?") + "geometryDefaultType=default";
    geoConn = (com.singlestore.jdbc.Connection) DriverManager.getConnection(binUrl);
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from PolygonCodec ORDER BY id");
    assertTrue(rs.next());
    return rs;
  }

  private CompleteResult getPrepare(com.singlestore.jdbc.Connection con) throws SQLException {
    PreparedStatement stmt =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from PolygonCodec"
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
  }

  public void getObject(ResultSet rs, boolean defaultGeo) throws SQLException {
    assertEquals(true, isMatching(ls1, rs.getObject(1, Polygon.class)));
    assertFalse(rs.wasNull());

    assertEquals(true, isMatching(ls2, rs.getObject(2, Polygon.class)));
    assertFalse(rs.wasNull());

    assertEquals(true, isMatching(ls3, rs.getObject(3, Polygon.class)));
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
        "POLYGON((7.00000000 2.00000000, 9.00000000 3.00000000, 6.00000000 9.00000000, 4.00000000 9.00000000, 1.00000000 5.00000000, 1.00000000 1.00000000, 7.00000000 2.00000000))");
    testObject(rs, Boolean.class, true);
    testErrObject(rs, java.util.Date.class);
  }

  @Test
  public void getMetaData() throws SQLException {
    getMetaData(sharedConn);
  }

  private void getMetaData(com.singlestore.jdbc.Connection con) throws SQLException {
    ResultSet rs = getPrepare(con);
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("CHAR", meta.getColumnTypeName(1));
    assertEquals(sharedConn.getCatalog(), meta.getCatalogName(1));

    assertEquals(String.class.getName(), meta.getColumnClassName(1));
    assertEquals("t1alias", meta.getColumnLabel(1));
    assertEquals("t1", meta.getColumnName(1));
    assertEquals(Types.CHAR, meta.getColumnType(1));
    assertEquals(4, meta.getColumnCount());
    assertEquals(0, meta.getScale(1));
    assertEquals("", meta.getSchemaName(1));
  }

  // TODO: PLAT-5913
  @Test
  public void sendParam() throws Exception {
    sendParam(sharedConn);
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE PolygonCodec2");

    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO PolygonCodec2(id, t1) VALUES (?, ?)")) {
      prep.setInt(1, 1);
      prep.setObject(2, ls1);
      prep.execute();

      prep.setInt(1, 2);
      prep.setObject(2, (Polygon) null);
      prep.execute();

      prep.setInt(1, 3);
      prep.setObject(2, ls2);
      prep.addBatch();

      prep.setInt(1, 4);
      prep.setObject(2, ls1);
      prep.addBatch();

      prep.setInt(1, 5);
      prep.setObject(2, ls4);
      prep.addBatch();

      prep.executeBatch();
    }

    ResultSet rs =
        con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
            .executeQuery("SELECT * FROM PolygonCodec2 ORDER BY id");

    assertTrue(rs.next());
    assertEquals(true, isMatching(ls1, rs.getObject(2, Polygon.class)));

    rs.updateNull(2);
    rs.updateRow();
    assertNull(rs.getObject(2, Polygon.class));

    assertTrue(rs.next());
    assertNull(rs.getObject(2, Polygon.class));

    rs.updateObject(2, ls2);
    rs.updateRow();
    assertEquals(true, isMatching(ls2, rs.getObject(2, Polygon.class)));

    assertTrue(rs.next());
    assertEquals(true, isMatching(ls2, rs.getObject(2, Polygon.class)));

    assertTrue(rs.next());
    assertEquals(true, isMatching(ls1, rs.getObject(2, Polygon.class)));

    assertTrue(rs.next());
    assertEquals(true, isMatching(ls4, rs.getObject(2, Polygon.class)));
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
                  }),
              new LineString(
                  new Point[] {
                    new Point(10, 10),
                    new Point(20, 10),
                    new Point(20, 20),
                    new Point(10, 20),
                    new Point(10, 10)
                  })
            });
    assertEquals(testPoly, ls2);
    assertEquals(testPoly.hashCode(), ls2.hashCode());
    assertFalse(ls2.equals(null));
    assertFalse(ls2.equals(""));
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
                  }),
              new LineString(
                  new Point[] {
                    new Point(10, 10),
                    new Point(20, 10),
                    new Point(20, 20),
                    new Point(10, 20),
                    new Point(10, 10)
                  })
            }),
        ls2);
  }

  /**
   * This method is to compare the two Polygons which need to be provided as an input.
   *
   * @param Polygon
   * @return true if both Polygons has same lineStrings along with points. Or else return false
   */
  public boolean isMatching(Polygon polygon1, Polygon polygon2) {
    if ((polygon1 == null && polygon2 != null) || (polygon1 != null && polygon2 == null)) {
      return false;
    } else if ((polygon1 == null && polygon2 == null)
        || (polygon1.getLines() == null && polygon2.getLines() == null)) {
      return true;
    } else if ((polygon1.getLines() == null && polygon2.getLines() != null)
        || (polygon1.getLines() != null && polygon2.getLines() == null)
        || polygon1.getLines().length != polygon2.getLines().length) {
      return false;
    }

    /*
     * First ring (LineString) of both polygons has to be same otherwise Polygon
     * will be considered different.
     *
     * LineString's start and end point should be same. Otherwise, return false.
     */

    List<Point> lineStringPoints1 = this.validateLineString(polygon1.getLines()[0]);
    List<Point> lineStringPoints2 = this.validateLineString(polygon2.getLines()[0]);

    if (lineStringPoints1 == null
        || lineStringPoints2 == null
        || lineStringPoints1.size() != lineStringPoints2.size()
        || !comparePoints(lineStringPoints1, lineStringPoints2)) {
      return false;
    }

    List<List<Point>> polygonLineStringPointList = new ArrayList<List<Point>>();

    /* if first LineString of both Polygon is same then compare other LineStrings */
    for (int i = 1; i < polygon1.getLines().length; i++) {
      boolean lineMatch = false;
      List<Point> points1 = this.validateLineString(polygon1.getLines()[i]);

      if (points1 == null) {
        return false;
      }

      for (int j = 1; j < polygon2.getLines().length; j++) {
        List<Point> points2 =
            (polygonLineStringPointList.isEmpty() || polygonLineStringPointList.size() < j)
                ? validateLineString(polygon2.getLines()[j])
                : polygonLineStringPointList.get(j - 1);

        if (points2 == null) {
          return false;
        }
        if (polygonLineStringPointList.isEmpty() || polygonLineStringPointList.size() < j) {
          polygonLineStringPointList.add(j - 1, points2);
        }

        if (points1.size() == points2.size() && comparePoints(points1, points2)) {
          lineMatch = true;
          break;
        }
      }

      if (!lineMatch) {
        return false;
      }
    }
    return true;
  }

  /**
   * This method is to validate if LineStrig is valid or not. Also remove duplicate Points from the
   * LineString.
   *
   * @param LineString
   * @return updated List of Points after removing duplicates. Return null if LineString is invalid.
   */
  private List<Point> validateLineString(LineString ls) {
    /*
     * LineString is valid -
     * a. if it contains at least 3 unique values
     * b. if start and end point are same.
     */
    if (ls == null
        || ls.getPoints() == null
        || ls.getPoints().length < 4
        || !ls.getPoints()[0].equals(ls.getPoints()[ls.getPoints().length - 1])) {
      return null;
    }

    return checkAndRemoveDuplicates(ls);
  }

  /**
   * This method is used to delete the duplicate Points from the LineString which are same as either
   * 'start' or 'end' Point.
   *
   * @param LineString
   * @return updated List of Points after removing duplicate
   */
  private List<Point> checkAndRemoveDuplicates(LineString ls) {
    Point[] points = ls.getPoints();
    int left = 0;
    int right = points.length;

    while (points[left + 1].equals(points[0])) {
      left += 1;
    }
    while (points[right - 1].equals(points[0])) {
      right -= 1;
    }

    List<Point> pointList = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(points, left, right)));
    Set<Point> pointSet = new HashSet<Point>(pointList);

    // PointList (of a LineString) should contain only distinct values and at least 3 distinct
    // values.
    if (pointSet.size() == pointList.size() && pointSet.size() >= 3) {
      return pointList;
    } else {
      return null;
    }
  }

  /**
   * This method is to compare two List of Points.
   *
   * @param List<Point>
   * @param List<Point>
   * @return true if both List of Points are same and contains points either in same or reverse
   *     order.
   */
  private boolean comparePoints(List<Point> pointsLineString1, List<Point> pointsLineString2) {
    int i1 = 0;
    int i2 = pointsLineString2.indexOf(pointsLineString1.get(i1));
    if (i2 == -1) {
      return false;
    }
    int step2 = 1;

    // if pointsLineString2 is in reverse order we need to traverse it backwards
    if (pointsLineString1
        .get(1)
        .equals(
            pointsLineString2.get(
                (i2 == 0)
                    ? (pointsLineString2.size() - 1) % pointsLineString2.size()
                    : (i2 - 1) % pointsLineString2.size()))) {
      step2 = -1;
    }

    i1 += 1;
    i2 = (i2 + step2) % pointsLineString2.size();
    while (i1 != 0) {
      if (!pointsLineString1.get(i1).equals(pointsLineString2.get(i2))) {
        return false;
      }
      i1 = (i1 + 1) % pointsLineString1.size();
      i2 = (i2 + step2) % pointsLineString2.size();
    }
    return true;
  }
}
