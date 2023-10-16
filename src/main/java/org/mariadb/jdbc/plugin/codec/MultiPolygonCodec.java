// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.type.*;

/** MultiPolygon codec */
public class MultiPolygonCodec implements Codec<MultiPolygon> {

  /** default instance */
  public static final MultiPolygonCodec INSTANCE = new MultiPolygonCodec();

  public String className() {
    return MultiPolygon.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return column.getType() == DataType.GEOMETRY && type.isAssignableFrom(MultiPolygon.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof MultiPolygon;
  }

  @Override
  public MultiPolygon decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return decodeBinary(buf, length, column, cal);
  }

  @Override
  public MultiPolygon decodeBinary(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    if (column.getType() == DataType.GEOMETRY) {
      buf.skip(4); // SRID
      Geometry geo = Geometry.getGeometry(buf, length.get() - 4, column);
      if (geo instanceof MultiPolygon) return (MultiPolygon) geo;
      throw new SQLDataException(
          String.format(
              "Geometric type %s cannot be decoded as MultiPolygon", geo.getClass().getName()));
    }
    buf.skip(length.get());
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as MultiPolygon", column.getType()));
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(("ST_MPolyFromText('" + value.toString() + "')").getBytes());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    MultiPolygon mariadbMultiPolygon = (MultiPolygon) value;

    int length = 13;
    for (Polygon poly : mariadbMultiPolygon.getPolygons()) {
      length += 9;
      for (LineString ls : poly.getLines()) {
        length += 4 + ls.getPoints().length * 16;
      }
    }

    encoder.writeLength(length);
    encoder.writeInt(0); // SRID
    encoder.writeByte(0x01); // LITTLE ENDIAN
    encoder.writeInt(6); // wkbMultiPolygon
    encoder.writeInt(mariadbMultiPolygon.getPolygons().length); // nb polygon

    for (Polygon poly : mariadbMultiPolygon.getPolygons()) {
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

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
