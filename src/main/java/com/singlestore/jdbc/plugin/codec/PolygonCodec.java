// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.codec;

import com.singlestore.jdbc.client.Column;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.plugin.Codec;
import com.singlestore.jdbc.type.Polygon;
import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;

public class PolygonCodec implements Codec<Polygon> {

  public static final PolygonCodec INSTANCE = new PolygonCodec();

  public String className() {
    return Polygon.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return column.getType() == DataType.CHAR && type.isAssignableFrom(Polygon.class);
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
    if (column.getType() == DataType.CHAR) {
      String s = buf.readString(length);
      try {
        return new Polygon(s);
      } catch (IllegalArgumentException ex) {
        throw new SQLDataException(String.format("Failed to decode '%s' as Polygon", s));
      }
    }
    buf.skip(length);
    throw new SQLDataException(
        String.format("Data type %s cannot be decoded as Polygon", column.getType()));
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeBytes(("'" + value.toString() + "'").getBytes());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encodeBinaryAsString(encoder, value, maxLength);
  }

  public int getBinaryEncodeType() {
    return DataType.BLOB.get();
  }
}
