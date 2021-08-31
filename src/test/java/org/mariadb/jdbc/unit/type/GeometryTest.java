// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.unit.type;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.type.*;

public class GeometryTest {

  /* s must be an even-length string. */
  public static byte[] hexStringToByteArray(String s1) {
    String s = s1.replace(" ", "");
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] =
          (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  @Test
  public void testPointEncoding() throws SQLException {
    byte[] ptBytes = hexStringToByteArray("000000000140000000000000004010000000000000");
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), ptBytes, ptBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, ptBytes.length, null);
    assertEquals("POINT(2.0 4.0)", geo.toString());
    assertEquals(geo, geo);

    assertEquals(new Point(2, 4), geo);
    assertEquals(new Point(2, 4).hashCode(), geo.hashCode());
    assertNotEquals("wrong", geo);
    assertNotEquals(new Point(2, 5), geo);
  }

  @Test
  public void testLineStringEncoding() throws SQLException {
    String lineBigEndian =
        "00"
            + "00000002"
            + "00000003"
            + "0000000000000000"
            + "0000000000000000"
            + "00 00 00 00 00 00 00 00"
            + "40 24 00 00 00 00 00 00"
            + "40 24 00 00 00 00 00 00"
            + "00 00 00 00 00 00 00 00";
    byte[] lineBytes = hexStringToByteArray(lineBigEndian);
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), lineBytes, lineBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, lineBytes.length, null);
    assertEquals("LINESTRING(0.0 0.0,0.0 10.0,10.0 0.0)", geo.toString());
    assertEquals(geo, geo);
    assertEquals(
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true),
        geo);
    assertEquals(
        new LineString(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true)
            .hashCode(),
        geo.hashCode());
    assertNotEquals("wrong", geo);
    assertNotEquals(
        new LineString(new Point[] {new Point(1, 0), new Point(0, 10), new Point(10, 0)}, true),
        geo);
  }

  @Test
  public void testPolygonStringEncoding() throws SQLException {
    String polygonBigEndian =
        "00  "
            + "00 00 00 03  "
            + "00 00 00 01  "
            + "00 00 00 07  "
            + "3F F0 00 00 00 00 00 00  "
            + "3F F0 00 00 00 00 00 00  "
            + "3F F0 00 00 00 00 00 00  "
            + "40 14 00 00 00 00 00 00  "
            + "40 10 00 00 00 00 00 00  "
            + "40 22 00 00 00 00 00 00  "
            + "40 18 00 00 00 00 00 00  "
            + "40 22 00 00 00 00 00 00  "
            + "40 22 00 00 00 00 00 00  "
            + "40 08 00 00 00 00 00 00  "
            + "40 1C 00 00 00 00 00 00  "
            + "40 00 00 00 00 00 00 00  "
            + "3F F0 00 00 00 00 00 00  "
            + "3F F0 00 00 00 00 00 00";
    byte[] lineBytes = hexStringToByteArray(polygonBigEndian);
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), lineBytes, lineBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, lineBytes.length, null);
    assertEquals(
        "POLYGON((1.0 1.0,1.0 5.0,4.0 9.0,6.0 9.0,9.0 3.0,7.0 2.0,1.0 1.0))", geo.toString());
    assertEquals(geo, geo);
    assertEquals(
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
            }),
        geo);
    assertEquals(
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
                })
            .hashCode(),
        geo.hashCode());
    assertNotEquals("wrong", geo);
    assertNotEquals(
        new Polygon(
            new LineString[] {
              new LineString(new Point[] {new Point(1, 1), new Point(1, 5), new Point(1, 1)}, false)
            }),
        geo);
  }

  @Test
  public void testMultiPointEncoding() throws SQLException {
    byte[] ptBytes =
        hexStringToByteArray(
            "00"
                + "00 00 00 04"
                + "00 00 00 03 "
                + "00 "
                + "00 00 00 01"
                + "00 00 00 00 00 00 00 00"
                + "00 00 00 00 00 00 00 00"
                + "00 "
                + "00 00 00 01"
                + "00 00 00 00 00 00 00 00"
                + "40 24 00 00 00 00 00 00"
                + "00 "
                + "00 00 00 01"
                + "40 24 00 00 00 00 00 00"
                + "00 00 00 00 00 00 00 00 ");
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), ptBytes, ptBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, ptBytes.length, null);
    assertEquals("MULTIPOINT(0.0 0.0,0.0 10.0,10.0 0.0)", geo.toString());
    assertEquals(geo, geo);
    assertEquals(
        new MultiPoint(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}), geo);
    assertEquals(
        new MultiPoint(new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)})
            .hashCode(),
        geo.hashCode());
    assertNotEquals("wrong", geo);
    assertNotEquals(
        new MultiPoint(new Point[] {new Point(0, 0), new Point(0, 11), new Point(10, 0)}), geo);
  }

  @Test
  public void testMultiLinestringEncoding() throws SQLException {
    byte[] ptBytes =
        hexStringToByteArray(
            "00"
                + "00 00 00 05"
                + "00 00 00 01 "
                + "00 "
                + "00000002"
                + "00000003"
                + "00 00 00 00 00 00 00 00"
                + "00 00 00 00 00 00 00 00"
                + "00 00 00 00 00 00 00 00"
                + "40 24 00 00 00 00 00 00"
                + "40 24 00 00 00 00 00 00"
                + "00 00 00 00 00 00 00 00");
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), ptBytes, ptBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, ptBytes.length, null);
    assertEquals("MULTILINESTRING((0.0 0.0,0.0 10.0,10.0 0.0))", geo.toString());
    assertEquals(geo, geo);
    MultiLineString ml =
        new MultiLineString(
            new LineString[] {
              new LineString(
                  new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true)
            });
    assertEquals(ml, geo);
    assertEquals(ml.hashCode(), geo.hashCode());
    assertFalse(geo.equals("wrong"));
  }

  @Test
  public void testMultiPolygonEncoding() throws SQLException {
    byte[] ptBytes =
        hexStringToByteArray(
            "00"
                + "00 00 00 06"
                + "00 00 00 01 "
                + "00 "
                + "00 00 00 03  "
                + "00 00 00 01  "
                + "00 00 00 07  "
                + "3F F0 00 00 00 00 00 00  "
                + "3F F0 00 00 00 00 00 00  "
                + "3F F0 00 00 00 00 00 00  "
                + "40 14 00 00 00 00 00 00  "
                + "40 10 00 00 00 00 00 00  "
                + "40 22 00 00 00 00 00 00  "
                + "40 18 00 00 00 00 00 00  "
                + "40 22 00 00 00 00 00 00  "
                + "40 22 00 00 00 00 00 00  "
                + "40 08 00 00 00 00 00 00  "
                + "40 1C 00 00 00 00 00 00  "
                + "40 00 00 00 00 00 00 00  "
                + "3F F0 00 00 00 00 00 00  "
                + "3F F0 00 00 00 00 00 00");
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), ptBytes, ptBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, ptBytes.length, null);
    assertEquals(
        "MULTIPOLYGON(((1.0 1.0,1.0 5.0,4.0 9.0,6.0 9.0,9.0 3.0,7.0 2.0,1.0 1.0)))",
        geo.toString());
    assertEquals(geo, geo);
    MultiPolygon ml =
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
                        false)
                  })
            });
    assertEquals(ml, geo);
    assertEquals(ml.hashCode(), geo.hashCode());
    assertFalse(geo.equals("wrong"));
  }

  @Test
  public void testMultiGeoEncoding() throws SQLException {
    byte[] ptBytes =
        hexStringToByteArray(
            "00"
                + "00 00 00 07"
                + "00 00 00 02 "
                + "00 "
                + "00 00 00 01"
                + "40 00 00 00 00 00 00 00 "
                + "40 10 00 00 00 00 00 00"
                + "00"
                + "00000002"
                + "00000003"
                + "0000000000000000"
                + "0000000000000000"
                + "00 00 00 00 00 00 00 00"
                + "40 24 00 00 00 00 00 00"
                + "40 24 00 00 00 00 00 00"
                + "00 00 00 00 00 00 00 00");
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), ptBytes, ptBytes.length);
    Geometry geo = Geometry.getGeometry(readBuf, ptBytes.length, null);
    assertEquals(
        "GEOMETRYCOLLECTION(POINT(2.0 4.0),LINESTRING(0.0 0.0,0.0 10.0,10.0 0.0))", geo.toString());
    GeometryCollection geo1 =
        new GeometryCollection(
            new Geometry[] {
              new Point(2, 4),
              new LineString(
                  new Point[] {new Point(0, 0), new Point(0, 10), new Point(10, 0)}, true)
            });
    assertEquals(geo1, geo);
  }

  @Test
  public void testWrongEncoding() throws SQLException {
    byte[] ptBytes = hexStringToByteArray("00 00 00 00 08");
    ReadableByteBuf readBuf =
        new StandardReadableByteBuf(new MutableInt(), ptBytes, ptBytes.length);
    assertThrows(
        SQLException.class,
        () ->
            Geometry.getGeometry(
                readBuf, ptBytes.length, ColumnDefinitionPacket.create("test", DataType.GEOMETRY)));
    assertNull(
        Geometry.getGeometry(
            new StandardReadableByteBuf(new MutableInt(), new byte[0], 0), 0, null));
  }
}
