// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.UUID;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** UUID codec */
public class UuidCodec implements Codec<UUID> {

  /** default instance */
  public static final UuidCodec INSTANCE = new UuidCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(DataType.VARCHAR, DataType.VARSTRING, DataType.STRING);

  public String className() {
    return UUID.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(UUID.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof UUID;
  }

  public UUID decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return UUID.fromString(column.decodeStringText(buf, length, cal, context));
  }

  public UUID decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return UUID.fromString(column.decodeStringBinary(buf, length, cal, context));
  }

  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeAscii(value.toString());
    encoder.writeByte('\'');
  }

  public void encodeBinary(
      Writer writer, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    String valueSt = value.toString();
    writer.writeLength(valueSt.length());
    writer.writeAscii(valueSt);
  }

  public int getBinaryEncodeType() {
    return DataType.VARSTRING.get();
  }
}
