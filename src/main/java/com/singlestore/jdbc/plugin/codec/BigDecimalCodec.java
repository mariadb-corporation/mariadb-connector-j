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
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;

public class BigDecimalCodec implements Codec<BigDecimal> {

  public static final BigDecimalCodec INSTANCE = new BigDecimalCodec();
  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.BIT,
          DataType.DECIMAL,
          DataType.OLDDECIMAL,
          DataType.YEAR,
          DataType.DECIMAL,
          DataType.VARCHAR,
          DataType.CHAR,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return BigDecimal.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(BigDecimal.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof BigDecimal;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public BigDecimal decodeText(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case OLDDECIMAL:
      case YEAR:
        return new BigDecimal(buf.readAscii(length));

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as BigDecimal", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
        String str = buf.readString(length);
        try {
          return new BigDecimal(str);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' cannot be decoded as BigDecimal", str));
        }

      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigDecimal.valueOf(result);

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as BigDecimal", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public BigDecimal decodeBinary(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {

    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedByte());
        }
        return BigDecimal.valueOf((int) buf.readByte());

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedShort());
        }
        return BigDecimal.valueOf((int) buf.readShort());

      case MEDIUMINT:
        if (!column.isSigned()) {
          int val = buf.readUnsignedMedium();
          buf.skip();
          return BigDecimal.valueOf(val);
        }
        return BigDecimal.valueOf(buf.readInt());

      case INT:
        if (!column.isSigned()) {
          return BigDecimal.valueOf(buf.readUnsignedInt());
        }
        return BigDecimal.valueOf(buf.readInt());

      case BIGINT:
        BigInteger val;
        if (column.isSigned()) {
          val = BigInteger.valueOf(buf.readLong());
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          val = new BigInteger(1, bb);
        }

        return new BigDecimal(String.valueOf(val)).setScale(column.getDecimals());

      case FLOAT:
        return BigDecimal.valueOf(buf.readFloat());

      case DOUBLE:
        return BigDecimal.valueOf(buf.readDouble());

      case BIT:
        long result = 0;
        for (int i = 0; i < length; i++) {
          byte b = buf.readByte();
          result = (result << 8) + (b & 0xff);
        }
        return BigDecimal.valueOf(result);

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as BigDecimal", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if has a collation (this is TEXT column)

      case VARCHAR:
      case CHAR:
      case DECIMAL:
      case OLDDECIMAL:
        String str = buf.readString(length);
        try {
          return new BigDecimal(str);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' cannot be decoded as BigDecimal", str));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as BigDecimal", column.getType()));
    }
  }

  @Override
  public void encodeText(Writer encoder, Context context, Object value, Calendar cal, Long length)
      throws IOException {
    encoder.writeAscii(((BigDecimal) value).toPlainString());
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    String asciiFormat = ((BigDecimal) value).toPlainString();
    encoder.writeLength(asciiFormat.length());
    encoder.writeAscii(asciiFormat);
  }

  public int getBinaryEncodeType() {
    return DataType.DECIMAL.get();
  }
}
