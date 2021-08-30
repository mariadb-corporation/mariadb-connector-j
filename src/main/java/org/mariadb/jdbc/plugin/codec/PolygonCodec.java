// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.type.Geometry;
import org.mariadb.jdbc.type.LineString;
import org.mariadb.jdbc.type.Point;
import org.mariadb.jdbc.type.Polygon;

public class PolygonCodec implements Codec<Polygon> {

  public static final PolygonCodec INSTANCE = new PolygonCodec();

  public String className() {
    return Polygon.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(Polygon.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Polygon;
  }

  @Override
  public Polygon decodeText(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal);
  }

  @Override
  public Polygon decodeBinary(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length - 4, column);
      if (geo instanceof Polygon) return (Polygon) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as Polygon", geo.getClass().getName()));
    }
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Polygon", column.getType()));
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_PolyFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    Polygon poly = (Polygon) value;

    int length = 13;
    for (LineString ls : poly.getLines()) {
      length += 4 + ls.getPoints().length * 16;
    }

    encoder.writeLength(length);
    encoder.writeInt(0); // SRID
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

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
