// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.Calendar;
import java.util.Locale;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.util.CharsetEncodingLength;

/** Column metadata definition */
public class BlobColumn extends StringColumn implements ColumnDecoder {
  /**
   * Constructor for column corresponding to BLOB datatype. Class permit specific decoding for this
   * datatype
   *
   * @param buf Column definition MySQL packet buffer
   * @param charset charset
   * @param length datatype length
   * @param dataType data type
   * @param decimals number of decimals
   * @param flags column flags
   * @param stringPos string value position
   * @param extTypeName extended type name
   * @param extTypeFormat extended type format
   */
  public BlobColumn(
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

  protected BlobColumn(BlobColumn prev) {
    super(prev);
  }

  @Override
  public BlobColumn useAliasAsName() {
    return new BlobColumn(this);
  }

  public String defaultClassname(Configuration conf) {
    return isBinary() ? Blob.class.getName() : String.class.getName();
  }

  public int getColumnType(Configuration conf) {
    if (columnLength <= 0 || getDisplaySize() > 16777215) {
      return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    } else {
      if (dataType == DataType.TINYBLOB || dataType == DataType.BLOB) {
        return isBinary() ? Types.VARBINARY : Types.VARCHAR;
      }
      return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    }
  }

  public String getColumnTypeName(Configuration conf) {
    /*
     map to different blob types based on datatype length
     see https://mariadb.com/kb/en/library/data-types/
    */
    if (extTypeFormat != null) {
      return extTypeFormat.toUpperCase(Locale.ROOT);
    }
    if (isBinary()) {
      if (columnLength < 0) {
        return "LONGBLOB";
      } else if (columnLength <= 255) {
        return "TINYBLOB";
      } else if (columnLength <= 65535) {
        return "BLOB";
      } else if (columnLength <= 16777215) {
        return "MEDIUMBLOB";
      } else {
        return "LONGBLOB";
      }
    } else {
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
    }
  }

  public int getPrecision() {
    if (!isBinary()) {
      Integer maxWidth2 = CharsetEncodingLength.maxCharlen.get(charset);
      if (maxWidth2 != null) return (int) (columnLength / maxWidth2);
      return (int) columnLength / 4;
    }
    return (int) columnLength;
  }

  @Override
  public Object getDefaultText(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    if (isBinary()) {
      return buf.readBlob(length.get());
    }
    return buf.readString(length.get());
  }

  @Override
  public Object getDefaultBinary(final Configuration conf, ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return getDefaultText(conf, buf, length);
  }

  @Override
  public boolean decodeBooleanText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Boolean", dataType));
    }
    String s = buf.readAscii(length.get());
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(ReadableByteBuf buf, MutableInt length)
      throws SQLDataException {
    return decodeBooleanText(buf, length);
  }

  @Override
  public byte decodeByteText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    long result;
    if (!isBinary()) {
      // TEXT column
      String str2 = buf.readString(length.get());
      try {
        result = new BigDecimal(str2).setScale(0, RoundingMode.DOWN).longValue();
      } catch (NumberFormatException nfe) {
        throw new SQLDataException(
            String.format("value '%s' (%s) cannot be decoded as Byte", str2, dataType));
      }
      if ((byte) result != result) {
        throw new SQLDataException("byte overflow");
      }

      return (byte) result;
    }
    if (length.get() > 0) {
      byte b = buf.readByte();
      buf.skip(length.get() - 1);
      return b;
    }
    throw new SQLDataException("empty String value cannot be decoded as Byte");
  }

  @Override
  public byte decodeByteBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    return buf.readString(length.get());
  }

  @Override
  public short decodeShortText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Short", dataType));
    }
    return super.decodeShortText(buf, length);
  }

  @Override
  public short decodeShortBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Short", dataType));
    }
    return super.decodeShortBinary(buf, length);
  }

  @Override
  public int decodeIntText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Integer", dataType));
    }
    return super.decodeIntText(buf, length);
  }

  @Override
  public int decodeIntBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Integer", dataType));
    }
    return super.decodeIntBinary(buf, length);
  }

  @Override
  public long decodeLongText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
    }
    return super.decodeLongText(buf, length);
  }

  @Override
  public long decodeLongBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
    }
    return super.decodeLongBinary(buf, length);
  }

  @Override
  public float decodeFloatText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Float", dataType));
    }
    return super.decodeFloatText(buf, length);
  }

  @Override
  public float decodeFloatBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Float", dataType));
    }
    return super.decodeFloatText(buf, length);
  }

  @Override
  public double decodeDoubleText(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Double", dataType));
    }
    return super.decodeDoubleText(buf, length);
  }

  @Override
  public double decodeDoubleBinary(ReadableByteBuf buf, MutableInt length) throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Double", dataType));
    }
    return super.decodeDoubleBinary(buf, length);
  }

  @Override
  public Date decodeDateText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
    }
    return super.decodeDateText(buf, length, cal);
  }

  @Override
  public Date decodeDateBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
    }
    return super.decodeDateBinary(buf, length, cal);
  }

  @Override
  public Time decodeTimeText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
    }
    return super.decodeTimeText(buf, length, cal);
  }

  @Override
  public Time decodeTimeBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
    }
    return super.decodeTimeBinary(buf, length, cal);
  }

  @Override
  public Timestamp decodeTimestampText(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Timestamp", dataType));
    }
    return super.decodeTimestampText(buf, length, cal);
  }

  @Override
  public Timestamp decodeTimestampBinary(ReadableByteBuf buf, MutableInt length, Calendar cal)
      throws SQLDataException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Timestamp", dataType));
    }
    return super.decodeTimestampBinary(buf, length, cal);
  }
}
