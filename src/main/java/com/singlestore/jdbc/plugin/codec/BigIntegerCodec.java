// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.plugin.codec;

import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.DataType;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableInt;
import com.singlestore.jdbc.plugin.Codec;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class BigIntegerCodec implements Codec<BigInteger> {

  public static final BigIntegerCodec INSTANCE = new BigIntegerCodec();
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.BIGINT,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.FLOAT,
          DataType.BIT,
          DataType.VARCHAR,
          DataType.CHAR,
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
  public int getApproximateTextProtocolLength(Object value) {
    return canEncode(value) ? ((BigInteger) value).toByteArray().length : -1;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public BigInteger decodeText(
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
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
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
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
      case INT:
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
      ReadableByteBuf buf, MutableInt length, ColumnDecoder column, Calendar cal)
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
        return BigInteger.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigInteger.valueOf(buf.readUnsignedShort());
        }
        return BigInteger.valueOf((int) buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          int val = buf.readUnsignedMedium();
          buf.skip();
          return BigInteger.valueOf(val);
        }
        return BigInteger.valueOf(buf.readInt());

      case INT:
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
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
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
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException {
    encoder.writeAscii(value.toString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    String asciiFormat = value.toString();
    encoder.writeLength(asciiFormat.length());
    encoder.writeAscii(asciiFormat);
  }

  public int getBinaryEncodeType() {
    return DataType.DECIMAL.get();
  }
}
