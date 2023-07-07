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
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class ShortCodec implements Codec<Short> {

  public static final ShortCodec INSTANCE = new ShortCodec();

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

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Short.TYPE) || type.isAssignableFrom(Short.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Short;
  }

  public String className() {
    return Short.class.getName();
  }

  @Override
  public Short decodeText(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeTextShort(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public short decodeTextShort(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {
    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INT:
      case BIGINT:
      case YEAR:
        result = LongCodec.parseNotEmpty(buf, length);
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Short", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case CHAR:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Short", column.getType()));
    }

    if ((short) result != result || (result < 0 && !column.isSigned())) {
      throw new SQLDataException("Short overflow");
    }

    return (short) result;
  }

  @Override
  public Short decodeBinary(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeBinaryShort(buffer, length, column);
  }

  @SuppressWarnings("fallthrough")
  public short decodeBinaryShort(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {
    long result;
    switch (column.getType()) {
      case TINYINT:
        result = column.isSigned() ? buf.readByte() : buf.readUnsignedByte();
        break;

      case YEAR:
      case SMALLINT:
        result = column.isSigned() ? buf.readShort() : buf.readUnsignedShort();
        break;

      case MEDIUMINT:
        result = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        break;

      case INT:
        result = column.isSigned() ? buf.readInt() : buf.readUnsignedInt();
        break;

      case BIGINT:
        result = buf.readLong();
        if (result < 0 & !column.isSigned()) {
          throw new SQLDataException("int overflow");
        }
        break;

      case BIT:
        result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        break;

      case FLOAT:
        result = (long) buf.readFloat();
        break;

      case DOUBLE:
        result = (long) buf.readDouble();
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Short", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case OLDDECIMAL:
      case VARCHAR:
      case DECIMAL:
      case ENUM:
      case CHAR:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValueExact();
          break;
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Short", column.getType()));
    }

    if ((short) result != result || (result < 0 && !column.isSigned())) {
      throw new SQLDataException("Short overflow");
    }

    return (short) result;
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeShort((Short) value);
  }

  public int getBinaryEncodeType() {
    return DataType.SMALLINT.get();
  }
}
