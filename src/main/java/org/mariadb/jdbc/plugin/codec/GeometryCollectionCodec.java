// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.type.*;

/** GeometryCollection codec */
public class GeometryCollectionCodec implements Codec<GeometryCollection> {

  /** default instance */
  public static final GeometryCollectionCodec INSTANCE = new GeometryCollectionCodec();

  public String className() {
    return GeometryCollection.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(GeometryCollection.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof GeometryCollection;
  }

  @Override
  public GeometryCollection decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException {
    return decodeBinary(buf, length, column, cal, context);
  }

  @Override
  public GeometryCollection decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException, IOException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length.get() - 4, column);
      if (geo instanceof GeometryCollection) return (GeometryCollection) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as GeometryCollection",
              geo.getClass().getName()));
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as GeometryCollection", column.getType()));
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_GeomCollFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    GeometryCollection geometryCollection = (GeometryCollection) value;

    int length = 13;
    for (Geometry geo : geometryCollection.getGeometries()) {
      if (geo instanceof Point) {
        length += 21;
      } else if (geo instanceof LineString) {
        length += 9 + ((LineString) geo).getPoints().length * 16;
      } else if (geo instanceof Polygon) {
        length += 9;
        for (LineString ls : ((Polygon) geo).getLines()) {
          length += 4 + ls.getPoints().length * 16;
        }
      } else if (geo instanceof MultiPoint) {
        length += 9 + ((MultiPoint) geo).getPoints().length * 21;
      } else if (geo instanceof MultiLineString) {
        length += 9;
        for (LineString ls : ((MultiLineString) geo).getLines()) {
          length += 9 + ls.getPoints().length * 16;
        }
      } else if (geo instanceof MultiPolygon) {
        length += 9;
        for (Polygon poly : ((MultiPolygon) geo).getPolygons()) {
          length += 9;
          for (LineString ls : poly.getLines()) {
            length += 4 + ls.getPoints().length * 16;
          }
        }
      }
    }

    encoder.writeLength(length);
    encoder.writeInt(0); // SRID
    encoder.writeByte(0x01); // LITTLE ENDIAN
    encoder.writeInt(7); // wkbGeometryCollection
    encoder.writeInt(geometryCollection.getGeometries().length);
    for (Geometry geo : geometryCollection.getGeometries()) {
      if (geo instanceof Point) {
        Point pt = (Point) geo;
        encoder.writeByte(0x01); // LITTLE ENDIAN
        encoder.writeInt(1); // wkbPoint
        encoder.writeDouble(pt.getX());
        encoder.writeDouble(pt.getY());
      } else if (geo instanceof LineString) {
        LineString ls = (LineString) geo;
        encoder.writeByte(0x01); // LITTLE ENDIAN
        encoder.writeInt(2); // wkbLineString
        encoder.writeInt(ls.getPoints().length);
        for (Point pt : ls.getPoints()) {
          encoder.writeDouble(pt.getX());
          encoder.writeDouble(pt.getY());
        }
      } else if (geo instanceof Polygon) {
        Polygon poly = (Polygon) geo;
        encoder.writeByte(0x01); // LITTLE ENDIAN
        encoder.writeInt(3); // wkbPolygon
        encoder.writeInt(poly.getLines().length);
        for (LineString ls : poly.getLines()) {
          encoder.writeInt(ls.getPoints().length);
          for (Point pt : ls.getPoints()) {
            encoder.writeDouble(pt.getX());
            encoder.writeDouble(pt.getY());
          }
        }
      } else if (geo instanceof MultiPoint) {
        MultiPoint mp = (MultiPoint) geo;
        encoder.writeByte(0x01); // LITTLE ENDIAN
        encoder.writeInt(4); // wkbMultiPoint
        encoder.writeInt(mp.getPoints().length);
        for (Point pt : mp.getPoints()) {
          encoder.writeByte(0x01); // LITTLE ENDIAN
          encoder.writeInt(1); // wkbPoint
          encoder.writeDouble(pt.getX());
          encoder.writeDouble(pt.getY());
        }
      } else if (geo instanceof MultiLineString) {
        MultiLineString mlines = (MultiLineString) geo;
        encoder.writeByte(0x01); // LITTLE ENDIAN
        encoder.writeInt(5); // wkbMultiLineString
        encoder.writeInt(mlines.getLines().length);
        for (LineString ls : mlines.getLines()) {
          encoder.writeByte(0x01); // LITTLE ENDIAN
          encoder.writeInt(2); // wkbLineString
          encoder.writeInt(ls.getPoints().length); // nb points
          for (Point pt : ls.getPoints()) {
            encoder.writeDouble(pt.getX());
            encoder.writeDouble(pt.getY());
          }
        }
      } else if (geo instanceof MultiPolygon) {
        MultiPolygon multiPolygon = (MultiPolygon) geo;
        encoder.writeByte(0x01); // LITTLE ENDIAN
        encoder.writeInt(6); // wkbMultiPolygon
        encoder.writeInt(multiPolygon.getPolygons().length); // nb polygon

        for (Polygon poly : multiPolygon.getPolygons()) {
          encoder.writeByte(0x01); // LITTLE ENDIAN
          encoder.writeInt(3); // wkbPolygon
          encoder.writeInt(poly.getLines().length);
          for (LineString ls : poly.getLines()) {
            encoder.writeInt(ls.getPoints().length);
            for (Point pt : ls.getPoints()) {
              encoder.writeDouble(pt.getX());
              encoder.writeDouble(pt.getY());
            }
          }
        }
      }
    }
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
