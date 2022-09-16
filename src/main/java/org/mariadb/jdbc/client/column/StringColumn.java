// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.column;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLDataException;
import java.sql.Types;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.CharsetEncodingLength;

/** Column metadata definition */
public class StringColumn extends ColumnDefinitionPacket implements ColumnDecoder {

  public StringColumn(
      ReadableByteBuf buf,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      int[] stringPos,
      String extTypeName,
      String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  public String defaultClassname(Configuration conf) {
    return isBinary() ? "byte[]" : String.class.getName();
  }

  public int getColumnType(Configuration conf) {
    if (dataType == DataType.NULL) {
      return Types.NULL;
    }
    if (dataType == DataType.STRING) {
      return isBinary() ? Types.VARBINARY : Types.CHAR;
    }
    if (columnLength <= 0 || getDisplaySize() > 16777215) {
      return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    }
    return isBinary() ? Types.VARBINARY : Types.VARCHAR;
  }

  public String getColumnTypeName(Configuration conf) {
    switch (dataType) {
      case STRING:
        if (isBinary()) {
          return "BINARY";
        }
        return "CHAR";
      case VARSTRING:
      case VARCHAR:
        if (isBinary()) {
          return "VARBINARY";
        }
        if (columnLength < 0) {
          return "LONGTEXT";
        } else if (getDisplaySize() <= 65532) {
          return "VARCHAR";
        } else if (getDisplaySize() <= 65535) {
          return "TEXT";
        } else if (getDisplaySize() <= 16777215) {
          return "MEDIUMTEXT";
        } else {
          return "LONGTEXT";
        }
      default:
        return dataType.name();
    }
  }

  public int getPrecision() {
    Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
    if (maxWidth == null) {
      return (int) columnLength;
    }
    return (int) (columnLength / maxWidth);
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (isBinary()) {
      byte[] arr = new byte[length];
      buf.readBytes(arr);
      return arr;
    }
    return buf.readString(length);
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, int length)
      throws SQLDataException {
    if (isBinary()) {
      byte[] arr = new byte[length];
      buf.readBytes(arr);
      return arr;
    }
    return buf.readString(length);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, int length) throws SQLDataException {
    return !"0".equals(buf.readAscii(length));
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return !"0".equals(buf.readAscii(length));
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, int length) throws SQLDataException {
    String str = buf.readString(length);
    long result;
    try {
      result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).longValue();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(
          String.format("value '%s' (%s) cannot be decoded as Byte", str, dataType));
    }
    if ((byte) result != result || (result < 0 && !isSigned())) {
      throw new SQLDataException("byte overflow");
    }
    return (byte) result;
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, int length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length);
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, int length) throws SQLDataException {
    String str = buf.readString(length);
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).shortValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Short", str));
    }
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeShortText(buf, length);
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, int length) throws SQLDataException {
    String str = buf.readString(length);
    try {
      return new BigDecimal(str).setScale(0, RoundingMode.DOWN).intValueExact();
    } catch (NumberFormatException | ArithmeticException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Integer", str));
    }
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeIntText(buf, length);
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, int length) throws SQLDataException {
    String str = buf.readString(length);
    try {
      return new BigInteger(str).longValueExact();
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Long", str));
    }
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeLongText(buf, length);
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, int length) throws SQLDataException {
    String val = buf.readString(length);
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Float", val));
    }
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeFloatText(buf, length);
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, int length) throws SQLDataException {
    String str2 = buf.readString(length);
    try {
      return Double.parseDouble(str2);
    } catch (NumberFormatException nfe) {
      throw new SQLDataException(String.format("value '%s' cannot be decoded as Double", str2));
    }
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, int length) throws SQLDataException {
    return decodeDoubleText(buf, length);
  }
}
