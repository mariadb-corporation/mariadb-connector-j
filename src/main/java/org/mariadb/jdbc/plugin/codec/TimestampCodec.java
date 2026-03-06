// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;

/** Timestamp codec */
public class TimestampCodec extends UtilDateCodec {

  /** default instance */
  public static final TimestampCodec INSTANCE = new TimestampCodec();

  public String className() {
    return Timestamp.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Timestamp.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Timestamp;
  }

  @Override
  protected int getMicroseconds(java.util.Date val) {
    if (val instanceof Timestamp) {
      return ((Timestamp) val).getNanos() / 1000;
    }
    return super.getMicroseconds(val);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public java.util.Date decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeTimestampText(buf, length, cal, context);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public java.util.Date decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeTimestampBinary(buf, length, cal, context);
  }
}
