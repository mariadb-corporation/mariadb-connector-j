// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.type.Geometry;
import org.mariadb.jdbc.type.LineString;
import org.mariadb.jdbc.type.MultiLineString;
import org.mariadb.jdbc.type.Point;

/** MultiLineString codec */
public class MultiLinestringCodec implements Codec<MultiLineString> {

  /** default instance */
  public static final MultiLinestringCodec INSTANCE = new MultiLinestringCodec();

  public String className() {
    return MultiLineString.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(MultiLineString.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof MultiLineString;
  }

  @Override
  public MultiLineString decodeText(
      ReadableByteBuf buf, int length, ColumnDecoder column, Calendar cal) throws SQLDataException {
    return decodeBinary(buf, length, column, cal);
  }

  @Override
  public MultiLineString decodeBinary(
      ReadableByteBuf buf, int length, ColumnDecoder column, Calendar cal) throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length - 4, column);
      if (geo instanceof MultiLineString) return (MultiLineString) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as MultiLineString", geo.getClass().getName()));
    }
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as MultiLineString", column.getType()));
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_MLineFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    MultiLineString mp = (MultiLineString) value;

    int length = 13;
    for (LineString ls : mp.getLines()) {
      length += 9 + ls.getPoints().length * 16;
    }

    encoder.writeLength(length);
    encoder.writeInt(0); // SRID
    encoder.writeByte(0x01); // LITTLE ENDIAN
    encoder.writeInt(5); // wkbMultiLineString
    encoder.writeInt(mp.getLines().length);
    for (LineString ls : mp.getLines()) {
      encoder.writeByte(0x01); // LITTLE ENDIAN
      encoder.writeInt(2); // wkbLineString
      encoder.writeInt(ls.getPoints().length); // nb points
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
