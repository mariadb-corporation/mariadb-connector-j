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
import org.mariadb.jdbc.type.*;

/** LineString codec */
public class LineStringCodec implements Codec<LineString> {

  /** default instance */
  public static final LineStringCodec INSTANCE = new LineStringCodec();

  public String className() {
    return LineString.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(LineString.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof LineString;
  }

  @Override
  public LineString decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal, context);
  }

  @Override
  public LineString decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length.get() - 4, column);
      if (geo instanceof LineString) return (LineString) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as LineString", geo.getClass().getName()));
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as LineString", column.getType()));
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_LineFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    LineString line = (LineString) value;

    encoder.writeLength(13 + line.getPoints().length * 16L);
    encoder.writeInt(0); // SRID
    encoder.writeByte(0x01); // LITTLE ENDIAN
    encoder.writeInt(2); // wkbLineString
    encoder.writeInt(line.getPoints().length);
    for (Point pt : line.getPoints()) {
      encoder.writeDouble(pt.getX());
      encoder.writeDouble(pt.getY());
    }
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
