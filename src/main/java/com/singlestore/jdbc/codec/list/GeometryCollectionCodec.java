// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.codec.list;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.socket.PacketWriter;
import com.singlestore.jdbc.codec.Codec;
import com.singlestore.jdbc.codec.DataType;
import com.singlestore.jdbc.message.server.ColumnDefinitionPacket;
import com.singlestore.jdbc.type.*;
import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;

public class GeometryCollectionCodec implements Codec<GeometryCollection> {

  public static final GeometryCollectionCodec INSTANCE = new GeometryCollectionCodec();

  public String className() {
    return GeometryCollection.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(GeometryCollection.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof GeometryCollection;
  }

  @Override
  public GeometryCollection decodeText(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal);
  }

  @Override
  public GeometryCollection decodeBinary(
      ReadableByteBuf buf, int length, ColumnDefinitionPacket column, Calendar cal)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length - 4, column);
      if (geo instanceof GeometryCollection) return (GeometryCollection) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as GeometryCollection",
              geo.getClass().getName()));
    }
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as GeometryCollection", column.getType()));
  }

  @Override
  public void encodeText(
      PacketWriter encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_GeomCollFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(PacketWriter encoder, Object value, Calendar cal, Long maxLength)
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
