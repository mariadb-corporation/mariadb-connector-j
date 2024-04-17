// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.type.Geometry;
import org.mariadb.jdbc.type.LineString;
import org.mariadb.jdbc.type.Point;
import org.mariadb.jdbc.type.Polygon;

/** Polygon codec */
public class PolygonCodec implements Codec<Polygon> {

  /** default instance */
  public static final PolygonCodec INSTANCE = new PolygonCodec();

  public String className() {
    return Polygon.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(Polygon.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Polygon;
  }

  @Override
  public Polygon decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal, context);
  }

  @Override
  public Polygon decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length.get() - 4, column);
      if (geo instanceof Polygon) return (Polygon) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as Polygon", geo.getClass().getName()));
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Polygon", column.getType()));
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_PolyFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
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
