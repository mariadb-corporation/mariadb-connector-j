// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** Timestamp codec */
public class TimestampCodec implements Codec<Timestamp> {

  /** default instance */
  public static final TimestampCodec INSTANCE = new TimestampCodec();

  public String className() {
    return Timestamp.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return UtilDateCodec.COMPATIBLE_TYPES.contains(column.getType())
        && type.isAssignableFrom(Timestamp.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Timestamp;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Timestamp decodeText(
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
  public Timestamp decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeTimestampBinary(buf, length, cal, context);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Timestamp val, Calendar providedCal, Long maxLen)
      throws IOException {
    UtilDateCodec.encodeTextDate(encoder, context, val, providedCal, maxLen);
  }

  @Override
  public int getApproximateTextProtocolLength(Timestamp value, Long length) {
    return UtilDateCodec.getMicroseconds(value) > 0 ? 28 : 21;
  }

  @Override
  public void encodeBinary(
      Writer encoder, Context context, Timestamp value, Calendar providedCal, Long maxLength)
      throws IOException {
    UtilDateCodec.encodeBinaryDate(encoder, context, value, providedCal, maxLength);
  }

  public int getBinaryEncodeType() {
    return UtilDateCodec.INSTANCE.getBinaryEncodeType();
  }
}
