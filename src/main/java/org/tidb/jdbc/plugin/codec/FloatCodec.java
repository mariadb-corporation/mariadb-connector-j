// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.plugin.codec;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.tidb.jdbc.client.Column;
import org.tidb.jdbc.client.Context;
import org.tidb.jdbc.client.DataType;
import org.tidb.jdbc.client.ReadableByteBuf;
import org.tidb.jdbc.client.socket.Writer;
import org.tidb.jdbc.plugin.Codec;

/** Float codec */
public class FloatCodec implements Codec<Float> {

  /** default instance */
  public static final FloatCodec INSTANCE = new FloatCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.BIGINT,
          DataType.OLDDECIMAL,
          DataType.DECIMAL,
          DataType.YEAR,
          DataType.DOUBLE,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Float.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Float.TYPE) || type.isAssignableFrom(Float.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Float;
  }

  @Override
  public Float decodeText(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeTextFloat(buffer, length, column);
  }

  /**
   * Decode a float text encoded
   *
   * @param buf packet buffer
   * @param length data length
   * @param column column metadata
   * @return decoded float value
   * @throws SQLDataException if decoding exception
   */
  @SuppressWarnings("fallthrough")
  public float decodeTextFloat(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case FLOAT:
      case YEAR:
        return Float.parseFloat(buf.readAscii(length));

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Float", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String val = buf.readString(length);
        try {
          return Float.parseFloat(val);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", val));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Float", column.getType()));
    }
  }

  @Override
  public Float decodeBinary(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeBinaryFloat(buffer, length, column);
  }

  /**
   * Decode a float binary encoded
   *
   * @param buf packet buffer
   * @param length data length
   * @param column column metadata
   * @return decoded float value
   * @throws SQLDataException if decoding exception
   */
  @SuppressWarnings("fallthrough")
  public float decodeBinaryFloat(ReadableByteBuf buf, int length, Column column)
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
          return (float) buf.readUnsignedShort();
        }
        return buf.readShort();

      case MEDIUMINT:
        float f = column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium();
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return f;

      case INTEGER:
        if (!column.isSigned()) {
          return (float) buf.readUnsignedInt();
        }
        return (float) buf.readInt();

      case BIGINT:
        if (column.isSigned()) {
          return (float) buf.readLong();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          return new BigInteger(1, bb).floatValue();
        }

      case FLOAT:
        return buf.readFloat();

      case DOUBLE:
        return (float) buf.readDouble();

      case OLDDECIMAL:
      case DECIMAL:
        return new BigDecimal(buf.readAscii(length)).floatValue();

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (column.isBinary()) {
          buf.skip(length);
          throw new SQLDataException(
              String.format("Data type %s cannot be decoded as Float", column.getType()));
        }
        // expected fallthrough
        // BLOB is considered as String if it has a collation (this is TEXT column)

      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str2 = buf.readString(length);
        try {
          return Float.parseFloat(str2);
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", str2));
        }

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Float", column.getType()));
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
    encoder.writeFloat((Float) value);
  }

  public int getBinaryEncodeType() {
    return DataType.FLOAT.get();
  }
}
