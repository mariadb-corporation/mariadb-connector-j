// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.Column;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Writer;
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
  public static long parseBit(ReadableByteBuf buf, int length) {
    if (length == 1) {
      return buf.readUnsignedByte();
    }
    long val = 0;
    int idx = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length);
      idx++;
    } while (idx < length);
    return val;
  }

  public String className() {
    return Byte.class.getName();
  }

  public boolean canDecode(Column column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType())
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Object value) {
    return value instanceof Byte;
  }

  @Override
  public Byte decodeText(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeTextByte(buffer, length, column);
  }

  /**
   * Decode byte from packet
   *
   * @param buf packet buffer
   * @param length encoded length
   * @param column column metadata
   * @return byte value
   * @throws SQLDataException if any decoding error occurs
   */
  public byte decodeTextByte(ReadableByteBuf buf, int length, Column column)
      throws SQLDataException {

    long result;
    switch (column.getType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        result = LongCodec.parseNotEmpty(buf, length);
        break;

      case BIT:
        byte val = buf.readByte();
        if (length > 1) buf.skip(length - 1);
        return val;

      case FLOAT:
      case DOUBLE:
      case OLDDECIMAL:
      case DECIMAL:
      case ENUM:
      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Byte", str, column.getType()));
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (!column.isBinary()) {
          String str2 = buf.readString(length);
          try {
            result = new BigDecimal(str2).setScale(0, RoundingMode.DOWN).byteValueExact();
          } catch (NumberFormatException | ArithmeticException nfe) {
            throw new SQLDataException(
                String.format("value '%s' (%s) cannot be decoded as Byte", str2, column.getType()));
          }
          break;
        }
        if (length > 0) {
          byte b = buf.readByte();
          buf.skip(length - 1);
          return b;
        }
        throw new SQLDataException("empty String value cannot be decoded as Byte");

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", column.getType()));
    }

    if ((byte) result != result || (result < 0 && !column.isSigned())) {
      throw new SQLDataException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public Byte decodeBinary(
      final ReadableByteBuf buffer, final int length, final Column column, final Calendar cal)
      throws SQLDataException {
    return decodeBinaryByte(buffer, length, column);
  }

  /**
   * Decode byte from packet
   *
   * @param buf packet buffer
   * @param length encoded length
   * @param column column metadata
   * @return byte value
   * @throws SQLDataException if any decoding error occurs
   */
  public byte decodeBinaryByte(ReadableByteBuf buf, int length, Column column)
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

      case INTEGER:
        result = column.isSigned() ? buf.readInt() : buf.readUnsignedInt();
        break;

      case BIGINT:
        if (column.isSigned()) {
          result = buf.readLong();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          try {
            result = val.longValueExact();
          } catch (NumberFormatException | ArithmeticException nfe) {
            throw new SQLDataException(
                String.format("value '%s' (%s) cannot be decoded as Byte", val, column.getType()));
          }
        }
        break;

      case BIT:
        byte val = buf.readByte();
        if (length > 1) buf.skip(length - 1);
        return val;

      case FLOAT:
        result = (long) buf.readFloat();
        break;

      case DOUBLE:
        result = (long) buf.readDouble();
        break;

      case OLDDECIMAL:
      case DECIMAL:
      case ENUM:
      case VARCHAR:
      case VARSTRING:
      case STRING:
        String str = buf.readString(length);
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
        } catch (NumberFormatException nfe) {
          throw new SQLDataException(
              String.format("value '%s' (%s) cannot be decoded as Byte", str, column.getType()));
        }
        break;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (!column.isBinary()) {
          // TEXT column
          String str2 = buf.readString(length);
          try {
            result = new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValue();
          } catch (NumberFormatException nfe) {
            throw new SQLDataException(
                String.format("value '%s' (%s) cannot be decoded as Byte", str2, column.getType()));
          }
          break;
        }
        if (length > 0) {
          byte b = buf.readByte();
          buf.skip(length - 1);
          return b;
        }
        throw new SQLDataException("empty String value cannot be decoded as Byte");

      default:
        buf.skip(length);
        throw new SQLDataException(
            String.format("Data type %s cannot be decoded as Byte", column.getType()));
    }

    if ((byte) result != result) {
      throw new SQLDataException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeAscii(Integer.toString((Byte) value));
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar cal, Long maxLength)
      throws IOException {
    encoder.writeByte((byte) value);
  }

  public int getBinaryEncodeType() {
    return DataType.TINYINT.get();
  }
}
