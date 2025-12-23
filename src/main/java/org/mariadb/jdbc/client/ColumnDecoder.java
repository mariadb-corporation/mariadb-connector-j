// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.column.UuidColumn;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableInt;
import org.mariadb.jdbc.util.constants.ColumnFlags;

public interface ColumnDecoder extends Column {

  /**
   * Decode Column from mysql packet
   *
   * @param buf packet
   * @return column
   */
  static ColumnDecoder decodeStd(ReadableByteBuf buf) {
    // skip first strings
    int[] stringPos = new int[5];
    stringPos[0] = buf.skipIdentifier(); // schema pos
    stringPos[1] = buf.skipIdentifier(); // table alias pos
    stringPos[2] = buf.skipIdentifier(); // table pos
    stringPos[3] = buf.skipIdentifier(); // column alias pos
    stringPos[4] = buf.skipIdentifier(); // column pos
    buf.skipIdentifier();

    buf.skip(); // skip length always 0x0c
    short charset = buf.readShort();
    int length = buf.readInt();
    DataType dataType = DataType.of(buf.readUnsignedByte());
    int flags = buf.readUnsignedShort();
    byte decimals = buf.readByte();
    DataType.ColumnConstructor constructor =
        (flags & ColumnFlags.UNSIGNED) == 0
            ? dataType.getColumnConstructor()
            : dataType.getUnsignedColumnConstructor();
    return constructor.create(
        buf, charset, length, dataType, decimals, flags, stringPos, null, null);
  }

  /**
   * Decode Column from mysql packet
   *
   * @param buf packet
   * @return column
   */
  static ColumnDecoder decode(ReadableByteBuf buf) {
    // skip first strings
    int[] stringPos = new int[5];
    stringPos[0] = buf.skipIdentifier(); // schema pos
    stringPos[1] = buf.skipIdentifier(); // table alias pos
    stringPos[2] = buf.skipIdentifier(); // table pos
    stringPos[3] = buf.skipIdentifier(); // column alias pos
    stringPos[4] = buf.skipIdentifier(); // column pos
    buf.skipIdentifier();

    byte[] extTypeName = null;
    byte[] extTypeFormat = null;
    // fast skipping extended info (usually not set)
    if (buf.readByte() != 0) {
      // revert position, because has extended info.
      buf.pos(buf.pos() - 1);

      ReadableByteBuf subPacket = buf.readLengthBuffer();
      while (subPacket.readableBytes() > 0) {
        switch (subPacket.readByte()) {
          case 0:
            int nameLen = subPacket.readLength();
            extTypeName = new byte[nameLen];
            subPacket.readBytes(extTypeName);
            break;
          case 1:
            int formatLen = subPacket.readLength();
            extTypeFormat = new byte[formatLen];
            subPacket.readBytes(extTypeFormat);
            break;
          default: // skip data
            subPacket.skip(subPacket.readLength());
            break;
        }
      }
    }

    buf.skip(); // skip length always 0x0c
    short charset = buf.readShort();
    int length = buf.readInt();
    DataType dataType = DataType.of(buf.readUnsignedByte());
    int flags = buf.readUnsignedShort();
    byte decimals = buf.readByte();
    DataType.ColumnConstructor constructor =
        (extTypeName != null && extTypeName.length == 4
            && extTypeName[0] == 'u' && extTypeName[1] == 'u'
            && extTypeName[2] == 'i' && extTypeName[3] == 'd')
            ? UuidColumn::new
            : (flags & ColumnFlags.UNSIGNED) == 0
                ? dataType.getColumnConstructor()
                : dataType.getUnsignedColumnConstructor();
    return constructor.create(
        buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  /**
   * Create fake MySQL column definition packet with indicated datatype
   *
   * @param name column name
   * @param type data type
   * @param flags column flags
   * @return Column
   */
  static ColumnDecoder create(String database, String name, DataType type, int flags) {
    byte[] databaseBytes = (database == null ? "def" : database).getBytes(StandardCharsets.UTF_8);
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    byte[] arr = new byte[databaseBytes.length + 6 + 2 * nameBytes.length];

    // write catalog
    arr[0] = (byte) databaseBytes.length;
    System.arraycopy(databaseBytes, 0, arr, 1, databaseBytes.length);

    int[] stringPos = new int[5];
    stringPos[0] = databaseBytes.length + 1; // schema pos
    stringPos[1] = databaseBytes.length + 2; // table alias pos
    stringPos[2] = databaseBytes.length + 3; // table pos

    // lenenc_str     name
    // lenenc_str     org_name
    int pos = databaseBytes.length + 4;
    for (int i = 0; i < 2; i++) {
      stringPos[i + 3] = pos;
      arr[pos++] = (byte) nameBytes.length;
      System.arraycopy(nameBytes, 0, arr, pos, nameBytes.length);
      pos += nameBytes.length;
    }
    int len;

    /* Sensible predefined length - since we're dealing with I_S here, most char fields are 64 char long */
    switch (type) {
      case VARCHAR:
      case VARSTRING:
        len = 64 * 3; /* 3 bytes per UTF8 char */
        break;
      case SMALLINT:
        len = 5;
        break;
      case NULL:
        len = 0;
        break;
      default:
        len = 1;
        break;
    }
    DataType.ColumnConstructor constructor =
        (flags & ColumnFlags.UNSIGNED) == 0
            ? type.getColumnConstructor()
            : type.getUnsignedColumnConstructor();
    return constructor.create(
        new StandardReadableByteBuf(arr, arr.length),
        33,
        len,
        type,
        (byte) 0,
        flags,
        stringPos,
        null,
        null);
  }

  /**
   * Returns default class name depending on server column datatype
   *
   * @param conf configuration
   * @return default class name
   */
  String defaultClassname(Configuration conf);

  /**
   * Returns default java.sql.Types depending on server column datatype
   *
   * @param conf configuration
   * @return default java.sql.Types
   */
  int getColumnType(Configuration conf);

  /**
   * Returns server column datatype
   *
   * @param conf configuration
   * @return default server column datatype
   */
  String getColumnTypeName(Configuration conf);

  /**
   * Return decimal precision.
   *
   * @return decimal precision
   */
  default int getPrecision() {
    return (int) getColumnLength();
  }

  /**
   * Return default Object text encoded
   *
   * @param buf row buffer
   * @param length data length
   * @param context connection context
   * @return default Object
   * @throws SQLDataException if any decoding error occurs
   */
  Object getDefaultText(final ReadableByteBuf buf, final MutableInt length, Context context)
      throws SQLDataException;

  /**
   * Return default Object binary encoded
   *
   * @param buf row buffer
   * @param length data length
   * @param context connection context
   * @return default Object
   * @throws SQLDataException if any decoding error occurs
   */
  Object getDefaultBinary(final ReadableByteBuf buf, final MutableInt length, Context context)
      throws SQLDataException;

  /**
   * Return String text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection context
   * @return String value
   * @throws SQLDataException if any decoding error occurs
   */
  String decodeStringText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException;

  /**
   * Return String binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context current context
   * @return String value
   * @throws SQLDataException if any decoding error occurs
   */
  String decodeStringBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException;

  /**
   * Return byte text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return byte value
   * @throws SQLDataException if any decoding error occurs
   */
  byte decodeByteText(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Return byte binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return byte value
   * @throws SQLDataException if any decoding error occurs
   */
  byte decodeByteBinary(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Return date text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection Context
   * @return date value
   * @throws SQLDataException if any decoding error occurs
   */
  Date decodeDateText(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException;

  /**
   * Return date binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection Context
   * @return date value
   * @throws SQLDataException if any decoding error occurs
   */
  Date decodeDateBinary(
      final ReadableByteBuf buf, final MutableInt length, final Calendar cal, final Context context)
      throws SQLDataException;

  /**
   * Return time text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection context
   * @return time value
   * @throws SQLDataException if any decoding error occurs
   */
  Time decodeTimeText(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, Context context)
      throws SQLDataException;

  /**
   * Return time binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection context
   * @return time value
   * @throws SQLDataException if any decoding error occurs
   */
  Time decodeTimeBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, Context context)
      throws SQLDataException;

  /**
   * Return timestamp text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection context
   * @return timestamp value
   * @throws SQLDataException if any decoding error occurs
   */
  Timestamp decodeTimestampText(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, Context context)
      throws SQLDataException;

  /**
   * Return timestamp binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @param cal calendar
   * @param context connection context
   * @return timestamp value
   * @throws SQLDataException if any decoding error occurs
   */
  Timestamp decodeTimestampBinary(
      final ReadableByteBuf buf, final MutableInt length, Calendar cal, Context context)
      throws SQLDataException;

  /**
   * Return boolean text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return boolean value
   * @throws SQLDataException if any decoding error occurs
   */
  boolean decodeBooleanText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException;

  /**
   * Parse boolean binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return boolean value
   * @throws SQLDataException if any decoding error occurs
   */
  boolean decodeBooleanBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException;

  /**
   * Parse short text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return short value
   * @throws SQLDataException if any decoding error occurs
   */
  short decodeShortText(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Parse short binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return short value
   * @throws SQLDataException if any decoding error occurs
   */
  short decodeShortBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException;

  /**
   * Parse int text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return int value
   * @throws SQLDataException if any decoding error occurs
   */
  int decodeIntText(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Parse int binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return int value
   * @throws SQLDataException if any decoding error occurs
   */
  int decodeIntBinary(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Parse long text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return long value
   * @throws SQLDataException if any decoding error occurs
   */
  long decodeLongText(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Parse long binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return long value
   * @throws SQLDataException if any decoding error occurs
   */
  long decodeLongBinary(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Parse float text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return float value
   * @throws SQLDataException if any decoding error occurs
   */
  float decodeFloatText(final ReadableByteBuf buf, final MutableInt length) throws SQLDataException;

  /**
   * Parse float binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return float value
   * @throws SQLDataException if any decoding error occurs
   */
  float decodeFloatBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException;

  /**
   * Parse double text encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return double value
   * @throws SQLDataException if any decoding error occurs
   */
  double decodeDoubleText(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException;

  /**
   * Parse double binary encoded value
   *
   * @param buf row buffer
   * @param length data length
   * @return double value
   * @throws SQLDataException if any decoding error occurs
   */
  double decodeDoubleBinary(final ReadableByteBuf buf, final MutableInt length)
      throws SQLDataException;

  ColumnDecoder useAliasAsName();
}
