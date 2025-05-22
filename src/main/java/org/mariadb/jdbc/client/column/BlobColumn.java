// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.column;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.Calendar;
import java.util.Locale;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
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
      final BufferedReadableByteBuf buf,
      final int charset,
      final long length,
      final DataType dataType,
      final byte decimals,
      final int flags,
      final int[] stringPos,
      final String extTypeName,
      final String extTypeFormat) {
    super(buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  /**
   * Recreate new column using alias as name.
   *
   * @param prev current column
   */
  protected BlobColumn(BlobColumn prev) {
    super(prev);
  }

  @Override
  public int getDisplaySize() {
    if (charset != 63) {
      Integer maxWidth = CharsetEncodingLength.maxCharlen.get(charset);
      if (maxWidth != null) return (int) (columnLength / maxWidth);
      return (int) (columnLength / 4);
    }
    return (int) columnLength;
  }

  @Override
  public BlobColumn useAliasAsName() {
    return new BlobColumn(this);
  }

  @Override
  public String defaultClassname(final Configuration conf) {
    return isBinary() ? Blob.class.getName() : String.class.getName();
  }

  @Override
  public int getColumnType(final Configuration conf) {
    if (columnLength <= 0 || getDisplaySize() > 16777215) {
      return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    } else {
      if (dataType == DataType.TINYBLOB || dataType == DataType.BLOB) {
        return isBinary() ? Types.VARBINARY : Types.VARCHAR;
      }
      return isBinary() ? Types.LONGVARBINARY : Types.LONGVARCHAR;
    }
  }

  @Override
  public String getColumnTypeName(final Configuration conf) {
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
      } else if (getDisplaySize() <= 255) {
        return "TINYTEXT";
      } else if (getDisplaySize() <= 65535) {
        return "TEXT";
      } else if (getDisplaySize() <= 16777215) {
        return "MEDIUMTEXT";
      } else {
        return "LONGTEXT";
      }
    }
  }

  @Override
  public int getPrecision() {
    if (!isBinary()) {
      Integer maxWidth2 = CharsetEncodingLength.maxCharlen.get(charset);
      if (maxWidth2 != null) return (int) (columnLength / maxWidth2);
      return (int) columnLength / 4;
    }
    return (int) columnLength;
  }

  @Override
  public Object getDefaultText(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      byte[] bytes = new byte[length.get()];
      buf.readBytes(bytes);
      return bytes;
    }
    return buf.readString(length.get());
  }

  @Override
  public Object getDefaultBinary(
      final ReadableByteBuf buf, final MutableInt length, final Context context)
      throws SQLDataException, IOException {
    return getDefaultText(buf, length, context);
  }

  @Override
  public boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Boolean", dataType));
    }
    String s = buf.readAscii(length.get());
    return !"0".equals(s);
  }

  @Override
  public boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return decodeBooleanText(buf, length);
  }

  @Override
  public byte decodeByteText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
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
  public byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return decodeByteText(buf, length);
  }

  @Override
  public String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    return buf.readString(length.get());
  }

  @Override
  public String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    return buf.readString(length.get());
  }

  @Override
  public short decodeShortText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Short", dataType));
    }
    return super.decodeShortText(buf, length);
  }

  @Override
  public short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Short", dataType));
    }
    return super.decodeShortBinary(buf, length);
  }

  @Override
  public int decodeIntText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Integer", dataType));
    }
    return super.decodeIntText(buf, length);
  }

  @Override
  public int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Integer", dataType));
    }
    return super.decodeIntBinary(buf, length);
  }

  @Override
  public long decodeLongText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
    }
    return super.decodeLongText(buf, length);
  }

  @Override
  public long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Long", dataType));
    }
    return super.decodeLongBinary(buf, length);
  }

  @Override
  public float decodeFloatText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Float", dataType));
    }
    return super.decodeFloatText(buf, length);
  }

  @Override
  public float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    return this.decodeFloatText(buf, length);
  }

  @Override
  public double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Double", dataType));
    }
    return super.decodeDoubleText(buf, length);
  }

  @Override
  public double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Double", dataType));
    }
    return super.decodeDoubleBinary(buf, length);
  }

  @Override
  public Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
    }
    return super.decodeDateText(buf, length, cal, context);
  }

  @Override
  public Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Date", dataType));
    }
    return super.decodeDateBinary(buf, length, cal, context);
  }

  @Override
  public Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
    }
    return super.decodeTimeText(buf, length, cal, context);
  }

  @Override
  public Time decodeTimeBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(String.format("Data type %s cannot be decoded as Time", dataType));
    }
    return super.decodeTimeBinary(buf, length, cal, context);
  }

  @Override
  public Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Timestamp", dataType));
    }
    return super.decodeTimestampText(buf, length, cal, context);
  }

  @Override
  public Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, final Context context)
      throws SQLDataException, IOException {
    if (isBinary()) {
      buf.skip(length.get());
      throw new SQLDataException(
          String.format("Data type %s cannot be decoded as Timestamp", dataType));
    }
    return super.decodeTimestampBinary(buf, length, cal, context);
  }
}
