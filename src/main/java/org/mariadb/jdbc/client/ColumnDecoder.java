package org.mariadb.jdbc.client;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.util.constants.ColumnFlags;

public interface ColumnDecoder extends Column {
  String defaultClassname(Configuration conf);

  int getColumnType(Configuration conf);

  String getColumnTypeName(Configuration conf);

  default int getPrecision() {
    return (int) getColumnLength();
  }

  Object getDefaultText(final Configuration conf, final ReadableByteBuf buf, final int length)
      throws SQLDataException;

  Object getDefaultBinary(final Configuration conf, final ReadableByteBuf buf, final int length)
      throws SQLDataException;

  String decodeStringText(final ReadableByteBuf buf, final int length, final Calendar cal)
      throws SQLDataException;

  String decodeStringBinary(final ReadableByteBuf buf, final int length, final Calendar cal)
      throws SQLDataException;

  byte decodeByteText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  byte decodeByteBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  Date decodeDateText(final ReadableByteBuf buf, final int length, Calendar cal)
      throws SQLDataException;

  Date decodeDateBinary(final ReadableByteBuf buf, final int length, Calendar cal)
      throws SQLDataException;

  Time decodeTimeText(final ReadableByteBuf buf, final int length, Calendar cal)
      throws SQLDataException;

  Time decodeTimeBinary(final ReadableByteBuf buf, final int length, Calendar cal)
      throws SQLDataException;

  Timestamp decodeTimestampText(final ReadableByteBuf buf, final int length, Calendar cal)
      throws SQLDataException;

  Timestamp decodeTimestampBinary(final ReadableByteBuf buf, final int length, Calendar cal)
      throws SQLDataException;

  boolean decodeBooleanText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  boolean decodeBooleanBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  short decodeShortText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  short decodeShortBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  int decodeIntText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  int decodeIntBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  long decodeLongText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  long decodeLongBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  float decodeFloatText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  float decodeFloatBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  double decodeDoubleText(final ReadableByteBuf buf, final int length) throws SQLDataException;

  double decodeDoubleBinary(final ReadableByteBuf buf, final int length) throws SQLDataException;

  static ColumnDecoder decode(ReadableByteBuf buf, boolean extendedInfo) {
    // skip first strings
    int[] stringPos = new int[5];
    stringPos[0] = buf.skipIdentifier(); // schema pos
    stringPos[1] = buf.skipIdentifier(); // table alias pos
    stringPos[2] = buf.skipIdentifier(); // table pos
    stringPos[3] = buf.skipIdentifier(); // column alias pos
    stringPos[4] = buf.skipIdentifier(); // column pos
    buf.skipIdentifier();

    String extTypeName = null;
    String extTypeFormat = null;
    if (extendedInfo) {
      // fast skipping extended info (usually not set)
      if (buf.readByte() != 0) {
        // revert position, because has extended info.
        buf.pos(buf.pos() - 1);

        ReadableByteBuf subPacket = buf.readLengthBuffer();
        while (subPacket.readableBytes() > 0) {
          switch (subPacket.readByte()) {
            case 0:
              extTypeName = subPacket.readAscii(subPacket.readLength());
              break;
            case 1:
              extTypeFormat = subPacket.readAscii(subPacket.readLength());
              break;
            default: // skip data
              subPacket.skip(subPacket.readLength());
              break;
          }
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
        (flags & ColumnFlags.UNSIGNED) == 0
            ? dataType.getColumnConstructor()
            : dataType.getUnsignedColumnConstructor();
    return constructor.create(
        buf, charset, length, dataType, decimals, flags, stringPos, extTypeName, extTypeFormat);
  }

  static ColumnDecoder create(String name, DataType type, int flags) {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    byte[] arr = new byte[9 + 2 * nameBytes.length];
    arr[0] = 3;
    arr[1] = 'D';
    arr[2] = 'E';
    arr[3] = 'F';

    int[] stringPos = new int[5];
    stringPos[0] = 4; // schema pos
    stringPos[1] = 5; // table alias pos
    stringPos[2] = 6; // table pos

    // lenenc_str     name
    // lenenc_str     org_name
    int pos = 7;
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
}
