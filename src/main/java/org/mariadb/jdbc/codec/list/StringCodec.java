/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.codec.list;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.EnumSet;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.codec.Codec;
import org.mariadb.jdbc.codec.DataType;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;
import org.mariadb.jdbc.util.constants.ServerStatus;

public class StringCodec implements Codec<String> {

  public static final StringCodec INSTANCE = new StringCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.OLDDECIMAL,
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.INTEGER,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.TIMESTAMP,
          DataType.BIGINT,
          DataType.MEDIUMINT,
          DataType.DATE,
          DataType.TIME,
          DataType.DATETIME,
          DataType.YEAR,
          DataType.NEWDATE,
          DataType.JSON,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.SET,
          DataType.VARCHAR,
          DataType.VARSTRING,
          DataType.STRING);

  public String className() {
    return String.class.getName();
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getType()) && type.isAssignableFrom(String.class);
  }

  public boolean canEncode(Object value) {
    return value instanceof String;
  }

  public String decodeText(
      final ReadableByteBuf buf,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal) {
    if (column.getType() == DataType.BIT) {
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
      sb.append("b'");
      boolean firstByteNonZero = false;
      for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
        boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
        if (b) {
          sb.append('1');
          firstByteNonZero = true;
        } else if (firstByteNonZero) {
          sb.append('0');
        }
      }
      sb.append("'");
      return sb.toString();
    }
    return buf.readString(length);
  }

  public String decodeBinary(
      final ReadableByteBuf buf,
      final int length,
      final ColumnDefinitionPacket column,
      final Calendar cal) {
    switch (column.getType()) {
      case BIT:
        byte[] bytes = new byte[length];
        buf.readBytes(bytes);
        StringBuilder sb = new StringBuilder(bytes.length * Byte.SIZE + 3);
        sb.append("b'");
        boolean firstByteNonZero = false;
        for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
          boolean b = (bytes[i / Byte.SIZE] & 1 << (Byte.SIZE - 1 - (i % Byte.SIZE))) > 0;
          if (b) {
            sb.append('1');
            firstByteNonZero = true;
          } else if (firstByteNonZero) {
            sb.append('0');
          }
        }
        sb.append("'");
        return sb.toString();

      case TINYINT:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedByte());
        }
        return String.valueOf(buf.readByte());

      case YEAR:
        StringBuilder s = new StringBuilder(String.valueOf(buf.readUnsignedShort()));
        while (s.length() < column.getLength()) s.insert(0, "0");
        return s.toString();

      case SMALLINT:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedShort());
        }
        return String.valueOf(buf.readShort());

      case MEDIUMINT:
        String mediumStr =
            String.valueOf(column.isSigned() ? buf.readMedium() : buf.readUnsignedMedium());
        buf.skip(); // MEDIUMINT is encoded on 4 bytes in exchanges !
        return mediumStr;

      case INTEGER:
        if (!column.isSigned()) {
          return String.valueOf(buf.readUnsignedInt());
        }
        return String.valueOf(buf.readInt());

      case BIGINT:
        BigInteger val;
        if (column.isSigned()) {
          val = BigInteger.valueOf(buf.readLong());
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int ii = 7; ii >= 0; ii--) {
            bb[ii] = buf.readByte();
          }
          val = new BigInteger(1, bb);
        }

        return new BigDecimal(String.valueOf(val)).setScale(column.getDecimals()).toPlainString();

      case FLOAT:
        return String.valueOf(buf.readFloat());

      case DOUBLE:
        return String.valueOf(buf.readDouble());

      case TIME:
        long tDays = 0;
        int tHours = 0;
        int tMinutes = 0;
        int tSeconds = 0;
        long tMicroseconds = 0;
        boolean negate = false;

        if (length > 0) {
          negate = buf.readByte() == 0x01;
          if (length > 4) {
            tDays = buf.readUnsignedInt();
            if (length > 7) {
              tHours = buf.readByte();
              tMinutes = buf.readByte();
              tSeconds = buf.readByte();
              if (length > 8) {
                tMicroseconds = buf.readInt();
              }
            }
          }
        }
        int totalHour = (int) (tDays * 24 + tHours);
        String stTime =
            (negate ? "-" : "")
                + (totalHour < 10 ? "0" : "")
                + totalHour
                + ":"
                + (tMinutes < 10 ? "0" : "")
                + tMinutes
                + ":"
                + (tSeconds < 10 ? "0" : "")
                + tSeconds;
        if (column.getDecimals() == 0) return stTime;
        StringBuilder stMicro = new StringBuilder(String.valueOf(tMicroseconds));
        while (stMicro.length() < column.getDecimals()) {
          stMicro.insert(0, "0");
        }
        return stTime + "." + stMicro;

      case DATE:
        int dateYear = buf.readUnsignedShort();
        int dateMonth = buf.readByte();
        int dateDay = buf.readByte();
        if (length > 4) {
          buf.skip(length - 4);
        }
        return LocalDate.of(dateYear, dateMonth, dateDay).toString();

      case DATETIME:
      case TIMESTAMP:
        int year = buf.readUnsignedShort();
        int month = buf.readByte();
        int day = buf.readByte();
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        long microseconds = 0;

        if (length > 4) {
          hour = buf.readByte();
          minutes = buf.readByte();
          seconds = buf.readByte();

          if (length > 7) {
            microseconds = buf.readUnsignedInt();
          }
        }
        LocalDateTime dateTime =
            LocalDateTime.of(year, month, day, hour, minutes, seconds)
                .plusNanos(microseconds * 1000);
        return dateTime.toLocalDate().toString() + ' ' + dateTime.toLocalTime().toString();

      default:
        return buf.readString(length);
    }
  }

  public void encodeText(
      PacketWriter encoder, Context context, String value, Calendar cal, Long maxLen)
      throws IOException {
    encoder.writeByte('\'');
    encoder.writeStringEscaped(
        maxLen == null ? value : value.substring(0, maxLen.intValue()),
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    encoder.writeByte('\'');
  }

  public void encodeBinary(PacketWriter writer, Context context, String value, Calendar cal)
      throws IOException {
    byte[] b = value.getBytes(StandardCharsets.UTF_8);
    writer.writeLength(b.length);
    writer.writeBytes(b);
  }

  public DataType getBinaryEncodeType() {
    return DataType.VARSTRING;
  }
}
