// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.plugin.Codec;

/** BigInteger codec */
public class BigIntegerCodec implements Codec<BigInteger> {

  /** default instance */
  public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.FLOAT,
          DataType.BIT,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return BigInteger.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(BigInteger.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigInteger;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public BigInteger decodeText(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {

    switch (column.getType()) {
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case OLDDECIMAL:
        return new BigDecimal(buf.readAscii(length.get())).toBigInteger();

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as BigInteger", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readString(length.get());
        try {
          return new BigDecimal(str2).toBigInteger();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' cannot be decoded as BigInteger", str2));
        }

      case BIT:
        long result = 0;
        for (int i = 0; i < length.get(); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigInteger.valueOf(result);

      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        return new BigInteger(buf.readAscii(length.get()));

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as BigInteger", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public BigInteger decodeBinary(
      final ReadableByteBuf buf,
      final MutableInt length,
      final ColumnDecoder column,
      final Calendar cal,
      final Context context)
      throws SQLDataException {

    switch (column.getType()) {
      case BIT:
        long result = 0;
        for (int i = 0; i < length.get(); i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigInteger.valueOf(result);
      case TINYINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedByte());
        }
        return BigInteger.valueOf(buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedShort());
        }
        return BigInteger.valueOf(buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          int val = buf.readUnsignedMedium();
          buf.skip();
          return BigInteger.valueOf(val);
        }
        return BigInteger.valueOf(buf.readInt());

      case INTEGER:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedInt());
        }
        return BigInteger.valueOf(buf.readInt());

      case FLOAT:
        return BigDecimal.valueOf(buf.readFloat()).toBigInteger();

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDouble()).toBigInteger();

      case DECIMAL:
        return new BigDecimal(buf.readAscii(length.get())).toBigInteger();

      case BIGINT:
        if (column.isSigned()) return BigInteger.valueOf(buf.readLong());

        // need BIG ENDIAN, so reverse order
        byte[] bb = new byte[8];
        for (int i = 7; i >= 0; i--) {
          bb[i] = buf.readByte();
        }
        return new BigInteger(1, bb);

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length.get());
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as BigInteger", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length.get());
        try {
          return new BigInteger(str);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' cannot be decoded as BigInteger", str));
        }

      default:
        buf.skip(length.get());
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as BigInteger", column.getType()));
    }
  }

  @Override
  public void encodeText(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long length)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(
      final Writer encoder,
      final Context context,
      final Object value,
      final Calendar cal,
      final Long maxLength)
      throws IOException {
    String asciiFormat = value.toString();
    encoder.writeLength(asciiFormat.length());
    encoder.writeAscii(asciiFormat);
  }

  public int getBinaryEncodeType() {
    return DataType.DECIMAL.get();
  }
}
