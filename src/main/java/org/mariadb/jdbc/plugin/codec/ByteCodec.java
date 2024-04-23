// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** Byte codec */
public class ByteCodec implements Codec<Byte> {

  /** default instance */
  public static final ByteCodec INSTANCE = new ByteCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.BIT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.VARCHAR);

  /**
   * Parse Bits value to long value
   *
   * @param buf packet buffer
   * @param length encoded length
   * @return long value
   */
  public static long parseBit(ReadableByteBuf buf, MutableInt length) {
    if (length.get() == 1) {
      return buf.readUnsignedByte();
    }
    long val = 0;
    int idx = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length.get());
      idx++;
    } while (idx < length.get());
    return val;
  }

  public String className() {
    return Byte.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public Byte decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeByteText(buf, length);
  }

  @Override
  public Byte decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {
    return column.decodeByteBinary(buf, length);
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    encoder.writeAscii(Integer.toString((Byte) value));
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    encoder.writeByte((byte) value);
  }

  public int getBinaryEncodeType() {
    return DataType.TINYINT.get();
  }
}
