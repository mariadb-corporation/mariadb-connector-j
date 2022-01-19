// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;

/** Double codec */
public class DoubleCodec implements Codec<Double> {

  /** default instance */
  public static final DoubleCodec INSTANCE = new DoubleCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Double.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Double.TYPE) || type.isAssignableFrom(Double.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Double;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Double decodeText(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    return decodeTextDouble(buf, length, column);
  }
  /**
   * Decode Double from text data
   *
   * @param buf packet buffer
   * @param length data length
   * @param column column meta
   * @return double value
   * @throws SQLDataException if decoding error
   */
  @SuppressWarnings("fallthrough")
  public double decodeTextDouble(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case YEAR:
        return Double.parseDouble(buf.readAscii(length));

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Double", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readString(length);
        try {
          return Double.parseDouble(str2);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Double", column.getType()));
    }
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Double decodeBinary(ReadableByteBuf buf, int length, Column column, Calendar cal)
      throws SQLDataException {
    return decodeBinaryDouble(buf, length, column);
  }

  /**
   * Decode Double from binary data
   *
   * @param buf packet buffer
   * @param length data length
   * @param column column meta
   * @return double value
   * @throws SQLDataException if decoding error
   */
  @SuppressWarnings("fallthrough")
  public double decodeBinaryDouble(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {
    switch (column.getType()) {
      case TINYINT:
        if (!column.isSigned()) {
          return buf.readUnsignedByte();
        }
        return buf.readByte();

      case YEAR:
      case SMALLINT:
        if (!column.isSigned()) {
          return buf.readUnsignedShort();
        }
        return buf.readShort();

      case MEDIUMINT:
        double d;
        d = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return d;

      case INTEGER:
        if (!column.isSigned()) {
          return (double) buf.readUnsignedInt();
        }
        return buf.readInt();

      case BIGINT:
        if (column.isSigned()) {
          return (double) buf.readLong();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          return new BigInteger(1, bb).doubleValue();
        }

      case FLOAT:
        return buf.readFloat();

      case DOUBLE:
        return buf.readDouble();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readAscii(length)).doubleValue();

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Double", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readString(length);
        try {
          return Double.parseDouble(str2);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Double", column.getType()));
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
    encoder.writeDouble((Double) value);
  }

  public int getBinaryEncodeType() {
    return DataType.DOUBLE.get();
  }
}
