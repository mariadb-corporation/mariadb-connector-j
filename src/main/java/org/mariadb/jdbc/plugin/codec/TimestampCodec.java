// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.codec;

import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.plugin.Codec;

/** Timestamp codec */
public class TimestampCodec implements Codec<Timestamp> {

  /** default instance */
  public static final TimestampCodec INSTANCE = new TimestampCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.DATE,
          DataType.NEWDATE,
          DataType.DATETIME,
          DataType.TIMESTAMP,
          DataType.YEAR,
          DataType.VARSTRING,
          DataType.VARCHAR,
          DataType.STRING,
          DataType.TIME,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB);

  public String className() {
    return Timestamp.class.getName();
  }

  public boolean canDecode(ColumnDecoder column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(Timestamp.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof Timestamp;
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Timestamp decodeText(ReadableByteBuf buf, int length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeTimestampText(buf, length, cal);
  }

  @Override
  @SuppressWarnings("fallthrough")
  public Timestamp decodeBinary(ReadableByteBuf buf, int length, ColumnDecoder column, Calendar cal)
      throws SQLDataException {
    return column.decodeTimestampBinary(buf, length, cal);
  }

  @Override
  public void encodeText(
      Writer encoder, Context context, Object val, Calendar providedCal, Long maxLen)
      throws IOException {
    Timestamp ts = (Timestamp) val;
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(cal.getTimeZone());
    String dateString = sdf.format(ts);

    encoder.writeByte('\'');
    encoder.writeAscii(dateString);

    int microseconds = ts.getNanos() / 1000;
    if (microseconds > 0) {
      if (microseconds % 1000 == 0) {
        encoder.writeAscii("." + Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        encoder.writeAscii("." + Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    encoder.writeByte('\'');
  }

  @Override
  public void encodeBinary(Writer encoder, Object value, Calendar providedCal, Long maxLength)
      throws IOException {
    Timestamp ts = (Timestamp) value;
    Calendar cal = providedCal == null ? Calendar.getInstance() : providedCal;
    synchronized (cal) {
      cal.clear();
      cal.setTimeInMillis(ts.getTime());
      if (ts.getNanos() == 0) {
        encoder.writeByte(7); // length
        encoder.writeShort((short) cal.get(Calendar.YEAR));
        encoder.writeByte((cal.get(Calendar.MONTH) + 1));
        encoder.writeByte(cal.get(Calendar.DAY_OF_MONTH));
        encoder.writeByte(cal.get(Calendar.HOUR_OF_DAY));
        encoder.writeByte(cal.get(Calendar.MINUTE));
        encoder.writeByte(cal.get(Calendar.SECOND));
      } else {
        encoder.writeByte(11); // length
        encoder.writeShort((short) cal.get(Calendar.YEAR));
        encoder.writeByte((cal.get(Calendar.MONTH) + 1));
        encoder.writeByte(cal.get(Calendar.DAY_OF_MONTH));
        encoder.writeByte(cal.get(Calendar.HOUR_OF_DAY));
        encoder.writeByte(cal.get(Calendar.MINUTE));
        encoder.writeByte(cal.get(Calendar.SECOND));
        encoder.writeInt(ts.getNanos() / 1000);
      }
    }
  }

  public int getBinaryEncodeType() {
    return DataType.DATETIME.get();
  }
}
