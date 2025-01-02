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

/** Point codec */
public class PointCodec implements Codec<Point> {

  /** default instance */
  public static final PointCodec INSTANCE = new PointCodec();

  public String className() {
    return Point.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(Point.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Point;
  }

  @Override
  public Point decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal, context);
  }

  @Override
  public Point decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length.get() - 4, column);
      if (geo instanceof Point) return (Point) geo;
      throw new SQLDataException(
          String.format("Geometric type %s cannot be decoded as Point", geo.getClass().getName()));
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Point", column.getType()));
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_PointFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    Point pt = (Point) value;
    encoder.writeLength(25);
    encoder.writeInt(0); // SRID
    encoder.writeByte(0x01); // LITTLE ENDIAN
    encoder.writeInt(1); // wkbPoint
    encoder.writeDouble(pt.getX());
    encoder.writeDouble(pt.getY());
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
