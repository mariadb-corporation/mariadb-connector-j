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
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class LongCodec implements Codec<Long> {

  public static final LongCodec INSTANCE = new LongCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.VARCHAR,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.CHAR,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.BIGINT,
          DataType.BIT,
          DataType.YEAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Long.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Integer.TYPE) || type.isAssignableFrom(Long.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Long;
  }

  @Override
  public Long decodeText(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeTextLong(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public long decodeTextLong(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INT:
      case YEAR:
        return buf.atoi(length);

      case BIGINT:
        if (column.isSigned() || length < 10) {
          return buf.atoi(length);
        } else {
          BigInteger val = new BigInteger(buf.readAscii(length));
          try {
            return val.longValueExact();
          } catch (ArithmeticException ae) {
            throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
          }
        }

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return result;

      case DOUBLE:
      case FLOAT:
      case DECIMAL:
        String str2 = buf.readAscii(length);
        try {
          return new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str2));
        }

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Long", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
        String str = buf.readString(length);
        try {
          return new BigInteger(str).longValueExact();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Long", column.getType()));
    }
  }

  @Override
  public Long decodeBinary(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeBinaryLong(buffer, length, column);
  }

  /**
   * Decode long from binary row.
   *
   * @param buf packet buffer
   * @param length data length
   * @param column column metadata
   * @return long value
   * @throws SQLDataException if type doesn't correspond / wrong data
   */
  @SuppressWarnings("fallthrough")
  public long decodeBinaryLong(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {

    switch (column.getType()) {
      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return result;

      case TINYINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedByte();
        }
        return (long) buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return (long) buf.readUnsignedShort();
        }
        return (long) buf.readShort();

      case MEDIUMINT:
        long l = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return l;

      case INT:
        if (!column.isSigned()) {
          return buf.readUnsignedInt();
        }
        return (long) buf.readInt();

      case BIGINT:
        if (column.isSigned() || (buf.getByte(buf.pos() + 7) & 0x80) == 0) {
          return buf.readLong();
        } else {
          // error too big to return a long
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          try {
            return val.longValueExact();
          } catch (ArithmeticException ae) {
            throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", val));
          }
        }

      case FLOAT:
        return (long) buf.readFloat();

      case DOUBLE:
        return (long) buf.readDouble();

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Long", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
      case OLDDECIMAL:
      case DECIMAL:
        String str = buf.readString(length);
        try {
          return new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Long", column.getType()));
    }
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeLong((Long) value);
  }

  public int getBinaryEncodeType() {
    return DataType.BIGINT.get();
  }
}
