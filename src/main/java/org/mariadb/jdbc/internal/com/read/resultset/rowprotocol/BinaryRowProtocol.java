/*
 *
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.com.read.resultset.rowprotocol;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.com.read.resultset.ColumnDefinition;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionFactory;
import org.mariadb.jdbc.util.Options;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

public class BinaryRowProtocol extends RowProtocol {

  private final ColumnDefinition[] columnDefinition;
  private final int columnInformationLength;

  /**
   * Constructor.
   *
   * @param columnDefinition column information.
   * @param columnInformationLength number of columns
   * @param maxFieldSize max field size
   * @param options connection options
   */
  public BinaryRowProtocol(
      ColumnDefinition[] columnDefinition,
      int columnInformationLength,
      int maxFieldSize,
      Options options) {
    super(maxFieldSize, options);
    this.columnDefinition = columnDefinition;
    this.columnInformationLength = columnInformationLength;
  }

  /**
   * Set length and pos indicator to asked index.
   *
   * @param newIndex index (0 is first).
   * @see <a href="https://mariadb.com/kb/en/mariadb/resultset-row/">Resultset row protocol
   *     documentation</a>
   */
  public void setPosition(int newIndex) {

    // check NULL-Bitmap that indicate if field is null
    if ((buf[1 + (newIndex + 2) / 8] & (1 << ((newIndex + 2) % 8))) != 0) {
      this.lastValueNull = BIT_LAST_FIELD_NULL;
      return;
    }

    // if not must parse data until reading the desired field
    if (index != newIndex) {
      int internalPos = this.pos;
      if (index == -1 || index > newIndex) {
        // if there wasn't previous non-null read field, or if last field was after searched index,
        // position is set on first field position.
        index = 0;
        internalPos = 1 + (columnInformationLength + 9) / 8; // 0x00 header + NULL-Bitmap length
      } else {
        // start at previous non-null field position if was before searched index
        index++;
        internalPos += length;
      }

      for (; index <= newIndex; index++) {
        if ((buf[1 + (index + 2) / 8] & (1 << ((index + 2) % 8))) == 0) {
          if (index != newIndex) {
            // skip bytes
            switch (columnDefinition[index].getColumnType()) {
              case BIGINT:
              case DOUBLE:
                internalPos += 8;
                break;

              case INTEGER:
              case MEDIUMINT:
              case FLOAT:
                internalPos += 4;
                break;

              case SMALLINT:
              case YEAR:
                internalPos += 2;
                break;

              case TINYINT:
                internalPos += 1;
                break;

              default:
                int type = this.buf[internalPos++] & 0xff;
                switch (type) {
                  case 251:
                    break;

                  case 252:
                    internalPos +=
                        2
                            + (0xffff
                                & (((buf[internalPos] & 0xff)
                                    + ((buf[internalPos + 1] & 0xff) << 8))));
                    break;

                  case 253:
                    internalPos +=
                        3
                            + (0xffffff
                                & ((buf[internalPos] & 0xff)
                                    + ((buf[internalPos + 1] & 0xff) << 8)
                                    + ((buf[internalPos + 2] & 0xff) << 16)));
                    break;

                  case 254:
                    internalPos +=
                        8
                            + ((buf[internalPos] & 0xff)
                                + ((long) (buf[internalPos + 1] & 0xff) << 8)
                                + ((long) (buf[internalPos + 2] & 0xff) << 16)
                                + ((long) (buf[internalPos + 3] & 0xff) << 24)
                                + ((long) (buf[internalPos + 4] & 0xff) << 32)
                                + ((long) (buf[internalPos + 5] & 0xff) << 40)
                                + ((long) (buf[internalPos + 6] & 0xff) << 48)
                                + ((long) (buf[internalPos + 7] & 0xff) << 56));
                    break;

                  default:
                    internalPos += type;
                    break;
                }
                break;
            }
          } else {
            // read asked field position and length
            switch (columnDefinition[index].getColumnType()) {
              case BIGINT:
              case DOUBLE:
                this.pos = internalPos;
                length = 8;
                this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                return;

              case INTEGER:
              case MEDIUMINT:
              case FLOAT:
                this.pos = internalPos;
                length = 4;
                this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                return;

              case SMALLINT:
              case YEAR:
                this.pos = internalPos;
                length = 2;
                this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                return;

              case TINYINT:
                this.pos = internalPos;
                length = 1;
                this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                return;

              default:
                // field with variable length
                int type = this.buf[internalPos++] & 0xff;
                switch (type) {
                  case 251:
                    // null length field
                    // must never occur
                    // null value are set in NULL-Bitmap, not send with a null length indicator.
                    throw new IllegalStateException(
                        "null data is encoded in binary protocol but NULL-Bitmap is not set");

                  case 252:
                    // length is encoded on 3 bytes (0xfc header + 2 bytes indicating length)
                    length =
                        0xffff & ((buf[internalPos++] & 0xff) + ((buf[internalPos++] & 0xff) << 8));
                    this.pos = internalPos;
                    this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                    return;

                  case 253:
                    // length is encoded on 4 bytes (0xfd header + 3 bytes indicating length)
                    length =
                        0xffffff
                            & ((buf[internalPos++] & 0xff)
                                + ((buf[internalPos++] & 0xff) << 8)
                                + ((buf[internalPos++] & 0xff) << 16));
                    this.pos = internalPos;
                    this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                    return;

                  case 254:
                    // length is encoded on 9 bytes (0xfe header + 8 bytes indicating length)
                    length =
                        (int)
                            ((buf[internalPos++] & 0xff)
                                + ((long) (buf[internalPos++] & 0xff) << 8)
                                + ((long) (buf[internalPos++] & 0xff) << 16)
                                + ((long) (buf[internalPos++] & 0xff) << 24)
                                + ((long) (buf[internalPos++] & 0xff) << 32)
                                + ((long) (buf[internalPos++] & 0xff) << 40)
                                + ((long) (buf[internalPos++] & 0xff) << 48)
                                + ((long) (buf[internalPos++] & 0xff) << 56));
                    this.pos = internalPos;
                    this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                    return;

                  default:
                    // length is encoded on 1 bytes (is then less than 251)
                    length = type;
                    this.pos = internalPos;
                    this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                    return;
                }
            }
          }
        }
      }
    }
    this.lastValueNull = length == NULL_LENGTH ? BIT_LAST_FIELD_NULL : BIT_LAST_FIELD_NOT_NULL;
  }

  /**
   * Get string from raw binary format.
   *
   * @param columnInfo column information
   * @param cal calendar
   * @param timeZone time zone
   * @return String value of raw bytes
   * @throws SQLException if conversion failed
   */
  public String getInternalString(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
      throws SQLException {
    if ((lastValueNull & BIT_LAST_FIELD_NULL) != 0) {
      return null;
    }

    switch (columnInfo.getColumnType()) {
      case STRING:
        if (getMaxFieldSize() > 0) {
          return new String(
                  buf, pos, Math.min(getMaxFieldSize() * 3, length), StandardCharsets.UTF_8)
              .substring(0, Math.min(getMaxFieldSize(), length));
        }
        return new String(buf, pos, length, StandardCharsets.UTF_8);

      case BIT:
        return String.valueOf(parseBit());
      case TINYINT:
        return zeroFillingIfNeeded(String.valueOf(getInternalTinyInt(columnInfo)), columnInfo);
      case SMALLINT:
        return zeroFillingIfNeeded(String.valueOf(getInternalSmallInt(columnInfo)), columnInfo);
      case INTEGER:
      case MEDIUMINT:
        return zeroFillingIfNeeded(String.valueOf(getInternalMediumInt(columnInfo)), columnInfo);
      case BIGINT:
        if (!columnInfo.isSigned()) {
          return zeroFillingIfNeeded(String.valueOf(getInternalBigInteger(columnInfo)), columnInfo);
        }
        return zeroFillingIfNeeded(String.valueOf(getInternalLong(columnInfo)), columnInfo);
      case DOUBLE:
        return zeroFillingIfNeeded(String.valueOf(getInternalDouble(columnInfo)), columnInfo);
      case FLOAT:
        return zeroFillingIfNeeded(String.valueOf(getInternalFloat(columnInfo)), columnInfo);
      case TIME:
        return getInternalTimeString(columnInfo);
      case DATE:
        Date date = getInternalDate(columnInfo, cal, timeZone);
        if (date == null) {
          return null;
        }
        return date.toString();
      case YEAR:
        if (options.yearIsDateType) {
          Date dateInter = getInternalDate(columnInfo, cal, timeZone);
          return (dateInter == null) ? null : dateInter.toString();
        }
        return String.valueOf(getInternalSmallInt(columnInfo));
      case TIMESTAMP:
      case DATETIME:
        Timestamp timestamp = getInternalTimestamp(columnInfo, cal, timeZone);
        if (timestamp == null) {
          return null;
        }
        return timestamp.toString();
      case DECIMAL:
      case OLDDECIMAL:
        BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
        return (bigDecimal == null) ? null : zeroFillingIfNeeded(bigDecimal.toString(), columnInfo);
      case GEOMETRY:
        return new String(buf, pos, length);
      case NULL:
        return null;
      default:
        if (getMaxFieldSize() > 0) {
          return new String(
                  buf, pos, Math.min(getMaxFieldSize() * 3, length), StandardCharsets.UTF_8)
              .substring(0, Math.min(getMaxFieldSize(), length));
        }
        return new String(buf, pos, length, StandardCharsets.UTF_8);
    }
  }

  /**
   * Get int from raw binary format.
   *
   * @param columnInfo column information
   * @return int value
   * @throws SQLException if column is not numeric or is not in Integer bounds.
   */
  public int getInternalInt(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return 0;
    }

    long value;
    switch (columnInfo.getColumnType()) {
      case BIT:
        value = parseBit();
        break;
      case TINYINT:
        value = getInternalTinyInt(columnInfo);
        break;
      case SMALLINT:
      case YEAR:
        value = getInternalSmallInt(columnInfo);
        break;
      case INTEGER:
      case MEDIUMINT:
        value =
            ((buf[pos] & 0xff)
                + ((buf[pos + 1] & 0xff) << 8)
                + ((buf[pos + 2] & 0xff) << 16)
                + ((buf[pos + 3] & 0xff) << 24));
        if (columnInfo.isSigned()) {
          return (int) value;
        } else if (value < 0) {
          value = value & 0xffffffffL;
        }
        break;
      case BIGINT:
        value = getInternalLong(columnInfo);
        break;
      case FLOAT:
        value = (long) getInternalFloat(columnInfo);
        break;
      case DOUBLE:
        value = (long) getInternalDouble(columnInfo);
        break;
      case DECIMAL:
      case OLDDECIMAL:
        BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
        rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, bigDecimal, columnInfo);
        return bigDecimal.intValue();
      case VARSTRING:
      case VARCHAR:
      case STRING:
        value = Long.parseLong(new String(buf, pos, length, StandardCharsets.UTF_8));
        break;
      default:
        throw new SQLException(
            "getInt not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
    rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value, columnInfo);
    return (int) value;
  }

  /**
   * Get long from raw binary format.
   *
   * @param columnInfo column information
   * @return long value
   * @throws SQLException if column is not numeric or is not in Long bounds (for big unsigned
   *     values)
   */
  public long getInternalLong(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return 0;
    }

    long value;
    switch (columnInfo.getColumnType()) {
      case BIT:
        return parseBit();
      case TINYINT:
        value = getInternalTinyInt(columnInfo);
        break;
      case SMALLINT:
      case YEAR:
        value = getInternalSmallInt(columnInfo);
        break;
      case INTEGER:
      case MEDIUMINT:
        value = getInternalMediumInt(columnInfo);
        break;
      case BIGINT:
        if (columnInfo.isSigned() || (buf[pos + 7] & 0x80) == 0) {
          return (buf[pos] & 0xff)
              | ((long) (buf[pos + 1] & 0xff) << 8)
              | ((long) (buf[pos + 2] & 0xff) << 16)
              | ((long) (buf[pos + 3] & 0xff) << 24)
              | ((long) (buf[pos + 4] & 0xff) << 32)
              | ((long) (buf[pos + 5] & 0xff) << 40)
              | ((long) (buf[pos + 6] & 0xff) << 48)
              | ((long) (buf[pos + 7] & 0xff) << 56);
        }
        BigInteger unsignedValue =
            new BigInteger(
                new byte[] {
                  0, // to indicate sign
                  buf[pos + 7],
                  buf[pos + 6],
                  buf[pos + 5],
                  buf[pos + 4],
                  buf[pos + 3],
                  buf[pos + 2],
                  buf[pos + 1],
                  buf[pos]
                });
        throw new SQLException(
            "Out of range value for column '"
                + columnInfo.getName()
                + "' : value "
                + unsignedValue
                + " is not in Long range",
            "22003",
            1264);
      case FLOAT:
        Float floatValue = getInternalFloat(columnInfo);
        if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
          throw new SQLException(
              "Out of range value for column '"
                  + columnInfo.getName()
                  + "' : value "
                  + floatValue
                  + " is not in Long range",
              "22003",
              1264);
        }
        return floatValue.longValue();
      case DOUBLE:
        Double doubleValue = getInternalDouble(columnInfo);
        if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
          throw new SQLException(
              "Out of range value for column '"
                  + columnInfo.getName()
                  + "' : value "
                  + doubleValue
                  + " is not in Long range",
              "22003",
              1264);
        }
        return doubleValue.longValue();
      case DECIMAL:
      case OLDDECIMAL:
        BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
        rangeCheck(Long.class, Long.MIN_VALUE, Long.MAX_VALUE, bigDecimal, columnInfo);
        return bigDecimal.longValue();
      case VARSTRING:
      case VARCHAR:
      case STRING:
        return Long.parseLong(new String(buf, pos, length, StandardCharsets.UTF_8));
      default:
        throw new SQLException(
            "getLong not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
    rangeCheck(Long.class, Long.MIN_VALUE, Long.MAX_VALUE, value, columnInfo);
    return value;
  }

  /**
   * Get float from raw binary format.
   *
   * @param columnInfo column information
   * @return float value
   * @throws SQLException if column is not numeric or is not in Float bounds.
   */
  public float getInternalFloat(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return 0;
    }

    long value;
    switch (columnInfo.getColumnType()) {
      case BIT:
        return parseBit();
      case TINYINT:
        value = getInternalTinyInt(columnInfo);
        break;
      case SMALLINT:
      case YEAR:
        value = getInternalSmallInt(columnInfo);
        break;
      case INTEGER:
      case MEDIUMINT:
        value = getInternalMediumInt(columnInfo);
        break;
      case BIGINT:
        if (columnInfo.isSigned() || (buf[pos + 7] & 0x80) == 0) {
          return (buf[pos] & 0xff)
              | ((long) (buf[pos + 1] & 0xff) << 8)
              | ((long) (buf[pos + 2] & 0xff) << 16)
              | ((long) (buf[pos + 3] & 0xff) << 24)
              | ((long) (buf[pos + 4] & 0xff) << 32)
              | ((long) (buf[pos + 5] & 0xff) << 40)
              | ((long) (buf[pos + 6] & 0xff) << 48)
              | ((long) (buf[pos + 7] & 0xff) << 56);
        }
        BigInteger unsignedValue =
            new BigInteger(
                new byte[] {
                  0, // to indicate sign
                  buf[pos + 7],
                  buf[pos + 6],
                  buf[pos + 5],
                  buf[pos + 4],
                  buf[pos + 3],
                  buf[pos + 2],
                  buf[pos + 1],
                  buf[pos]
                });
        return unsignedValue.floatValue();
      case FLOAT:
        int valueFloat =
            ((buf[pos] & 0xff)
                + ((buf[pos + 1] & 0xff) << 8)
                + ((buf[pos + 2] & 0xff) << 16)
                + ((buf[pos + 3] & 0xff) << 24));
        return Float.intBitsToFloat(valueFloat);
      case DOUBLE:
        return (float) getInternalDouble(columnInfo);
      case DECIMAL:
      case VARSTRING:
      case VARCHAR:
      case STRING:
      case OLDDECIMAL:
        try {
          return Float.valueOf(new String(buf, pos, length, StandardCharsets.UTF_8));
        } catch (NumberFormatException nfe) {
          SQLException sqlException =
              new SQLException(
                  "Incorrect format for getFloat for data field with type "
                      + columnInfo.getColumnType().getJavaTypeName(),
                  "22003",
                  1264,
                  nfe);
          throw sqlException;
        }
      default:
        throw new SQLException(
            "getFloat not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
    try {
      return Float.valueOf(String.valueOf(value));
    } catch (NumberFormatException nfe) {
      SQLException sqlException =
          new SQLException(
              "Incorrect format for getFloat for data field with type "
                  + columnInfo.getColumnType().getJavaTypeName(),
              "22003",
              1264,
              nfe);
      throw sqlException;
    }
  }

  /**
   * Get double from raw binary format.
   *
   * @param columnInfo column information
   * @return double value
   * @throws SQLException if column is not numeric or is not in Double bounds (unsigned columns).
   */
  public double getInternalDouble(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return 0;
    }
    switch (columnInfo.getColumnType()) {
      case BIT:
        return parseBit();
      case TINYINT:
        return getInternalTinyInt(columnInfo);
      case SMALLINT:
      case YEAR:
        return getInternalSmallInt(columnInfo);
      case INTEGER:
      case MEDIUMINT:
        return getInternalMediumInt(columnInfo);
      case BIGINT:
        if (columnInfo.isSigned() || (buf[pos + 7] & 0x80) == 0) {
          return (buf[pos] & 0xff)
              | ((long) (buf[pos + 1] & 0xff) << 8)
              | ((long) (buf[pos + 2] & 0xff) << 16)
              | ((long) (buf[pos + 3] & 0xff) << 24)
              | ((long) (buf[pos + 4] & 0xff) << 32)
              | ((long) (buf[pos + 5] & 0xff) << 40)
              | ((long) (buf[pos + 6] & 0xff) << 48)
              | ((long) (buf[pos + 7] & 0xff) << 56);
        }
        BigInteger unsignedValue =
            new BigInteger(
                new byte[] {
                  0, // to indicate sign
                  buf[pos + 7],
                  buf[pos + 6],
                  buf[pos + 5],
                  buf[pos + 4],
                  buf[pos + 3],
                  buf[pos + 2],
                  buf[pos + 1],
                  buf[pos]
                });
        return unsignedValue.doubleValue();
      case FLOAT:
        return getInternalFloat(columnInfo);
      case DOUBLE:
        long valueDouble =
            ((buf[pos] & 0xff)
                + ((long) (buf[pos + 1] & 0xff) << 8)
                + ((long) (buf[pos + 2] & 0xff) << 16)
                + ((long) (buf[pos + 3] & 0xff) << 24)
                + ((long) (buf[pos + 4] & 0xff) << 32)
                + ((long) (buf[pos + 5] & 0xff) << 40)
                + ((long) (buf[pos + 6] & 0xff) << 48)
                + ((long) (buf[pos + 7] & 0xff) << 56));
        return Double.longBitsToDouble(valueDouble);
      case DECIMAL:
      case VARSTRING:
      case VARCHAR:
      case STRING:
      case OLDDECIMAL:
        try {
          return Double.valueOf(new String(buf, pos, length, StandardCharsets.UTF_8));
        } catch (NumberFormatException nfe) {
          SQLException sqlException =
              new SQLException(
                  "Incorrect format for getDouble for data field with type "
                      + columnInfo.getColumnType().getJavaTypeName(),
                  "22003",
                  1264);
          //noinspection UnnecessaryInitCause
          sqlException.initCause(nfe);
          throw sqlException;
        }
      default:
        throw new SQLException(
            "getDouble not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
  }

  /**
   * Get BigDecimal from raw binary format.
   *
   * @param columnInfo column information
   * @return BigDecimal value
   * @throws SQLException if column is not numeric
   */
  public BigDecimal getInternalBigDecimal(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }

    switch (columnInfo.getColumnType()) {
      case BIT:
        return BigDecimal.valueOf(parseBit());
      case TINYINT:
        return BigDecimal.valueOf(getInternalTinyInt(columnInfo));
      case SMALLINT:
      case YEAR:
        return BigDecimal.valueOf(getInternalSmallInt(columnInfo));
      case INTEGER:
      case MEDIUMINT:
        return BigDecimal.valueOf(getInternalMediumInt(columnInfo));
      case BIGINT:
        if (columnInfo.isSigned() || (buf[pos + 7] & 0x80) == 0) {
          long value =
              (buf[pos] & 0xff)
                  | ((long) (buf[pos + 1] & 0xff) << 8)
                  | ((long) (buf[pos + 2] & 0xff) << 16)
                  | ((long) (buf[pos + 3] & 0xff) << 24)
                  | ((long) (buf[pos + 4] & 0xff) << 32)
                  | ((long) (buf[pos + 5] & 0xff) << 40)
                  | ((long) (buf[pos + 6] & 0xff) << 48)
                  | ((long) (buf[pos + 7] & 0xff) << 56);
          return BigDecimal.valueOf(value).setScale(columnInfo.getDecimals());
        }
        BigInteger unsignedValue =
            new BigInteger(
                new byte[] {
                  0, // to indicate sign
                  buf[pos + 7],
                  buf[pos + 6],
                  buf[pos + 5],
                  buf[pos + 4],
                  buf[pos + 3],
                  buf[pos + 2],
                  buf[pos + 1],
                  buf[pos]
                });
        return new BigDecimal(unsignedValue).setScale(columnInfo.getDecimals());
      case FLOAT:
        return BigDecimal.valueOf(getInternalFloat(columnInfo));
      case DOUBLE:
        return BigDecimal.valueOf(getInternalDouble(columnInfo));
      case DECIMAL:
      case VARSTRING:
      case VARCHAR:
      case STRING:
      case OLDDECIMAL:
        return new BigDecimal(new String(buf, pos, length, StandardCharsets.UTF_8));
      default:
        throw new SQLException(
            "getBigDecimal not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
  }

  /**
   * Get date from raw binary format.
   *
   * @param columnInfo column information
   * @param cal calendar
   * @param timeZone time zone
   * @return date value
   * @throws SQLException if column is not compatible to Date
   */
  @SuppressWarnings("deprecation")
  public Date getInternalDate(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
      throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      lastValueNull |= BIT_LAST_FIELD_NULL;
      return null;
    }
    int year;
    int month = 1;
    int day = 1;
    Calendar calendar;
    switch (columnInfo.getColumnType()) {
      case TIMESTAMP:
      case DATETIME:
        year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);
        month = buf[pos + 2];
        day = buf[pos + 3];
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (length > 4) {
          hour = buf[pos + 4];
          minutes = buf[pos + 5];
          seconds = buf[pos + 6];

          if (length > 7) {
            microseconds =
                ((buf[pos + 7] & 0xff)
                    + ((buf[pos + 8] & 0xff) << 8)
                    + ((buf[pos + 9] & 0xff) << 16)
                    + ((buf[pos + 10] & 0xff) << 24));
          }
        }

        if (year == 0 && month == 0 && day == 0) {
          lastValueNull |= BIT_LAST_FIELD_NULL;
          return null;
        }

        if (timeZone == null) {
          // legacy is to send timestamps with current driver timezone. So display, is immediate
          calendar = (cal != null) ? cal : Calendar.getInstance();
          synchronized (calendar) {
            calendar.clear();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, month - 1);
            calendar.set(Calendar.DAY_OF_MONTH, day);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return new Date(calendar.getTimeInMillis());
          }
        }

        // timestamp is saved in server timezone,
        LocalDateTime ldt =
            LocalDateTime.of(year, month, day, hour, minutes, seconds, microseconds * 1000);
        ZonedDateTime zdt =
            ldt.atZone(timeZone.toZoneId()).withZoneSameInstant(TimeZone.getDefault().toZoneId());

        calendar = cal != null ? cal : Calendar.getInstance();
        synchronized (calendar) {
          calendar.clear();
          calendar.set(Calendar.YEAR, zdt.getYear());
          calendar.set(Calendar.MONTH, zdt.getMonthValue() - 1);
          calendar.set(Calendar.DAY_OF_MONTH, zdt.getDayOfMonth());
          calendar.set(Calendar.HOUR_OF_DAY, 0);
          calendar.set(Calendar.MINUTE, 0);
          calendar.set(Calendar.SECOND, 0);
          calendar.set(Calendar.MILLISECOND, 0);
          return new Date(calendar.getTimeInMillis());
        }

      case TIME:
        throw new SQLException("Cannot read Date using a Types.TIME field");
      case STRING:
        String rawValue = new String(buf, pos, length, StandardCharsets.UTF_8);
        if ("0000-00-00".equals(rawValue)) {
          lastValueNull |= BIT_LAST_ZERO_DATE;
          return null;
        }

        return new Date(
            Integer.parseInt(rawValue.substring(0, 4)) - 1900,
            Integer.parseInt(rawValue.substring(5, 7)) - 1,
            Integer.parseInt(rawValue.substring(8, 10)));
      default:
        year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);

        if (length == 2 && columnInfo.getLength() == 2) {
          // YEAR(2) - deprecated
          if (year <= 69) {
            year += 2000;
          } else {
            year += 1900;
          }
        }

        if (length >= 4) {
          month = buf[pos + 2];
          day = buf[pos + 3];
        }

        return new Date(year - 1900, month - 1, day);
    }
  }

  /**
   * Get time from raw binary format.
   *
   * @param columnInfo column information
   * @param cal calendar
   * @param timeZone time zone
   * @return Time value
   * @throws SQLException if column cannot be converted to Time
   */
  public Time getInternalTime(ColumnDefinition columnInfo, Calendar cal, TimeZone timeZone)
      throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }

    Calendar calendar;
    int day = 0;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    int microseconds = 0;

    switch (columnInfo.getColumnType()) {
      case TIMESTAMP:
      case DATETIME:
        if (length == 0) {
          lastValueNull |= BIT_LAST_FIELD_NULL;
          return null;
        }
        int year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);
        int month = buf[pos + 2];
        day = buf[pos + 3];
        if (length > 4) {
          hour = buf[pos + 4];
          minutes = buf[pos + 5];
          seconds = buf[pos + 6];

          if (length > 7) {
            microseconds =
                ((buf[pos + 7] & 0xff)
                    + ((buf[pos + 8] & 0xff) << 8)
                    + ((buf[pos + 9] & 0xff) << 16)
                    + ((buf[pos + 10] & 0xff) << 24));
          }
        }

        if (timeZone == null) {
          calendar = cal != null ? cal : Calendar.getInstance();
          synchronized (calendar) {
            calendar.clear();
            calendar.set(Calendar.YEAR, 1970);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, hour);
            calendar.set(Calendar.MINUTE, minutes);
            calendar.set(Calendar.SECOND, seconds);
            calendar.set(Calendar.MILLISECOND, microseconds / 1_000);
            return new Time(calendar.getTimeInMillis());
          }
        }

        LocalDateTime ldt =
            LocalDateTime.of(year, month, day, hour, minutes, seconds, microseconds * 1000);
        ZonedDateTime zdt =
            ldt.atZone(timeZone.toZoneId()).withZoneSameInstant(TimeZone.getDefault().toZoneId());

        calendar = cal != null ? cal : Calendar.getInstance();
        synchronized (calendar) {
          calendar.clear();
          calendar.set(Calendar.YEAR, 1970);
          calendar.set(Calendar.MONTH, 0);
          calendar.set(Calendar.DAY_OF_MONTH, 1);
          calendar.set(Calendar.HOUR_OF_DAY, zdt.getHour());
          calendar.set(Calendar.MINUTE, zdt.getMinute());
          calendar.set(Calendar.SECOND, zdt.getSecond());
          calendar.set(Calendar.MILLISECOND, zdt.getNano() / 1_000_000);
          return new Time(calendar.getTimeInMillis());
        }

      case DATE:
        throw new SQLException("Cannot read Time using a Types.DATE field");

      default:
        calendar = cal != null ? cal : Calendar.getInstance();
        calendar.clear();

        boolean negate = false;
        if (length > 0) {
          negate = (buf[pos] & 0xff) == 0x01;
        }
        if (length > 4) {
          day =
              ((buf[pos + 1] & 0xff)
                  + ((buf[pos + 2] & 0xff) << 8)
                  + ((buf[pos + 3] & 0xff) << 16)
                  + ((buf[pos + 4] & 0xff) << 24));
        }
        if (length > 7) {
          hour = buf[pos + 5];
          minutes = buf[pos + 6];
          seconds = buf[pos + 7];
        }
        calendar.set(
            1970,
            Calendar.JANUARY,
            ((negate ? -1 : 1) * day) + 1,
            (negate ? -1 : 1) * hour,
            minutes,
            seconds);

        int nanoseconds = 0;
        if (length > 8) {
          nanoseconds =
              ((buf[pos + 8] & 0xff)
                  + ((buf[pos + 9] & 0xff) << 8)
                  + ((buf[pos + 10] & 0xff) << 16)
                  + ((buf[pos + 11] & 0xff) << 24));
        }

        calendar.set(Calendar.MILLISECOND, nanoseconds / 1000);

        return new Time(calendar.getTimeInMillis());
    }
  }

  /**
   * Get timestamp from raw binary format.
   *
   * @param columnInfo column information
   * @param userCalendar user calendar
   * @param timeZone time zone
   * @return timestamp value
   * @throws SQLException if column type is not compatible
   */
  public Timestamp getInternalTimestamp(
      ColumnDefinition columnInfo, Calendar userCalendar, TimeZone timeZone) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      lastValueNull |= BIT_LAST_FIELD_NULL;
      return null;
    }

    int year = 1970;
    int month = 0;
    int day = 0;
    int hour = 0;
    int minutes = 0;
    int seconds = 0;
    int microseconds = 0;

    Calendar calendar;
    switch (columnInfo.getColumnType()) {
      case TIME:
        calendar = userCalendar != null ? userCalendar : Calendar.getInstance();

        boolean negate = false;
        if (length > 0) {
          negate = (buf[pos] & 0xff) == 0x01;
        }
        if (length > 4) {
          day =
              ((buf[pos + 1] & 0xff)
                  + ((buf[pos + 2] & 0xff) << 8)
                  + ((buf[pos + 3] & 0xff) << 16)
                  + ((buf[pos + 4] & 0xff) << 24));
        }
        if (length > 7) {
          hour = buf[pos + 5];
          minutes = buf[pos + 6];
          seconds = buf[pos + 7];
        }

        if (length > 8) {
          microseconds =
              ((buf[pos + 8] & 0xff)
                  + ((buf[pos + 9] & 0xff) << 8)
                  + ((buf[pos + 10] & 0xff) << 16)
                  + ((buf[pos + 11] & 0xff) << 24));
        }
        year = 1970;
        month = 1;
        day = ((negate ? -1 : 1) * day) + 1;
        hour = (negate ? -1 : 1) * hour;
        break;

      case STRING:
      case VARSTRING:
        String rawValue = new String(buf, pos, length, StandardCharsets.UTF_8);
        if (rawValue.startsWith("0000-00-00 00:00:00")) {
          lastValueNull |= BIT_LAST_ZERO_DATE;
          return null;
        }

        if (rawValue.length() >= 4) {
          year = Integer.parseInt(rawValue.substring(0, 4));
          if (rawValue.length() >= 7) {
            month = Integer.parseInt(rawValue.substring(5, 7));
            if (rawValue.length() >= 10) {
              day = Integer.parseInt(rawValue.substring(8, 10));
              if (rawValue.length() >= 19) {
                hour = Integer.parseInt(rawValue.substring(11, 13));
                minutes = Integer.parseInt(rawValue.substring(14, 16));
                seconds = Integer.parseInt(rawValue.substring(17, 19));
              }
              microseconds = extractNanos(rawValue) / 1000000;
            }
          }
        }

        if (userCalendar != null) {
          calendar = userCalendar;
        } else if (timeZone != null) {
          calendar = Calendar.getInstance(timeZone);
        } else {
          calendar = Calendar.getInstance();
        }

        break;

      default:
        year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);
        month = buf[pos + 2];
        day = buf[pos + 3];
        if (length > 4) {
          hour = buf[pos + 4];
          minutes = buf[pos + 5];
          seconds = buf[pos + 6];

          if (length > 7) {
            microseconds =
                ((buf[pos + 7] & 0xff)
                    + ((buf[pos + 8] & 0xff) << 8)
                    + ((buf[pos + 9] & 0xff) << 16)
                    + ((buf[pos + 10] & 0xff) << 24));
          }
        }

        if (userCalendar != null) {
          calendar = userCalendar;
        } else if (timeZone != null && columnInfo.getColumnType() != ColumnType.DATE) {
          calendar = Calendar.getInstance(timeZone);
        } else {
          calendar = Calendar.getInstance();
        }
    }

    Timestamp tt;
    synchronized (calendar) {
      calendar.clear();
      calendar.set(year, month - 1, day, hour, minutes, seconds);
      tt = new Timestamp(calendar.getTimeInMillis());
    }
    tt.setNanos(microseconds * 1000);
    return tt;
  }

  /**
   * Get Object from raw binary format.
   *
   * @param columnInfo column information
   * @param timeZone time zone
   * @return Object value
   * @throws SQLException if column type is not compatible
   */
  public Object getInternalObject(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }

    switch (columnInfo.getColumnType()) {
      case BIT:
        if (columnInfo.getLength() == 1) {
          return buf[pos] != 0;
        }
        byte[] dataBit = new byte[length];
        System.arraycopy(buf, pos, dataBit, 0, length);
        return dataBit;
      case TINYINT:
        if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
          return buf[pos] != 0;
        }
        return getInternalInt(columnInfo);
      case INTEGER:
        if (!columnInfo.isSigned()) {
          return getInternalLong(columnInfo);
        }
        return getInternalInt(columnInfo);
      case BIGINT:
        if (!columnInfo.isSigned()) {
          return getInternalBigInteger(columnInfo);
        }
        return getInternalLong(columnInfo);
      case DOUBLE:
        return getInternalDouble(columnInfo);
      case VARCHAR:
      case VARSTRING:
      case STRING:
        if (columnInfo.isBinary()) {
          byte[] data = new byte[getLengthMaxFieldSize()];
          System.arraycopy(buf, pos, data, 0, getLengthMaxFieldSize());
          return data;
        }
        return getInternalString(columnInfo, null, timeZone);
      case TIMESTAMP:
      case DATETIME:
        return getInternalTimestamp(columnInfo, null, timeZone);
      case DATE:
        return getInternalDate(columnInfo, null, timeZone);
      case DECIMAL:
        return getInternalBigDecimal(columnInfo);
      case BLOB:
      case LONGBLOB:
      case MEDIUMBLOB:
      case TINYBLOB:
        byte[] dataBlob = new byte[getLengthMaxFieldSize()];
        System.arraycopy(buf, pos, dataBlob, 0, getLengthMaxFieldSize());
        return dataBlob;
      case NULL:
        return null;
      case YEAR:
        if (options.yearIsDateType) {
          return getInternalDate(columnInfo, null, timeZone);
        }
        return getInternalShort(columnInfo);
      case SMALLINT:
      case MEDIUMINT:
        return getInternalInt(columnInfo);
      case FLOAT:
        return getInternalFloat(columnInfo);
      case TIME:
        return getInternalTime(columnInfo, null, timeZone);
      case OLDDECIMAL:
      case JSON:
        return getInternalString(columnInfo, null, timeZone);
      case GEOMETRY:
        byte[] data = new byte[length];
        System.arraycopy(buf, pos, data, 0, length);
        return data;
      case ENUM:
        break;
      case NEWDATE:
        break;
      case SET:
        break;
      default:
        break;
    }
    throw ExceptionFactory.INSTANCE.notSupported(
        String.format("Type '%s' is not supported", columnInfo.getColumnType().getTypeName()));
  }

  /**
   * Get boolean from raw binary format.
   *
   * @param columnInfo column information
   * @return boolean value
   * @throws SQLException if column type doesn't permit conversion
   */
  public boolean getInternalBoolean(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return false;
    }
    switch (columnInfo.getColumnType()) {
      case BIT:
        return parseBit() != 0;
      case TINYINT:
        return getInternalTinyInt(columnInfo) != 0;
      case SMALLINT:
      case YEAR:
        return getInternalSmallInt(columnInfo) != 0;
      case INTEGER:
      case MEDIUMINT:
        return getInternalMediumInt(columnInfo) != 0;
      case BIGINT:
        return getInternalLong(columnInfo) != 0;
      case FLOAT:
        return getInternalFloat(columnInfo) != 0;
      case DOUBLE:
        return getInternalDouble(columnInfo) != 0;
      case DECIMAL:
      case OLDDECIMAL:
        return getInternalBigDecimal(columnInfo).longValue() != 0;
      default:
        final String rawVal = new String(buf, pos, length, StandardCharsets.UTF_8);
        return !("false".equals(rawVal) || "0".equals(rawVal));
    }
  }

  /**
   * Get byte from raw binary format.
   *
   * @param columnInfo column information
   * @return byte value
   * @throws SQLException if column type doesn't permit conversion
   */
  public byte getInternalByte(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return 0;
    }
    long value;
    switch (columnInfo.getColumnType()) {
      case BIT:
        value = parseBit();
        break;
      case TINYINT:
        value = getInternalTinyInt(columnInfo);
        break;
      case SMALLINT:
      case YEAR:
        value = getInternalSmallInt(columnInfo);
        break;
      case INTEGER:
      case MEDIUMINT:
        value = getInternalMediumInt(columnInfo);
        break;
      case BIGINT:
        value = getInternalLong(columnInfo);
        break;
      case FLOAT:
        value = (long) getInternalFloat(columnInfo);
        break;
      case DOUBLE:
        value = (long) getInternalDouble(columnInfo);
        break;
      case DECIMAL:
      case OLDDECIMAL:
        BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
        rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, bigDecimal, columnInfo);
        return bigDecimal.byteValue();
      case VARSTRING:
      case VARCHAR:
      case STRING:
        value = Long.parseLong(new String(buf, pos, length, StandardCharsets.UTF_8));
        break;
      default:
        throw new SQLException(
            "getByte not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
    rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, value, columnInfo);
    return (byte) value;
  }

  /**
   * Get short from raw binary format.
   *
   * @param columnInfo column information
   * @return short value
   * @throws SQLException if column type doesn't permit conversion
   */
  public short getInternalShort(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return 0;
    }

    long value;
    switch (columnInfo.getColumnType()) {
      case BIT:
        value = parseBit();
        break;
      case TINYINT:
        value = getInternalTinyInt(columnInfo);
        break;
      case SMALLINT:
      case YEAR:
        value = ((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8));
        if (columnInfo.isSigned()) {
          return (short) value;
        }
        value = value & 0xffff;
        break;
      case INTEGER:
      case MEDIUMINT:
        value = getInternalMediumInt(columnInfo);
        break;
      case BIGINT:
        value = getInternalLong(columnInfo);
        break;
      case FLOAT:
        value = (long) getInternalFloat(columnInfo);
        break;
      case DOUBLE:
        value = (long) getInternalDouble(columnInfo);
        break;
      case DECIMAL:
      case OLDDECIMAL:
        BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
        rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, bigDecimal, columnInfo);
        return bigDecimal.shortValue();
      case VARSTRING:
      case VARCHAR:
      case STRING:
        value = Long.parseLong(new String(buf, pos, length, StandardCharsets.UTF_8));
        break;
      default:
        throw new SQLException(
            "getShort not available for data field type "
                + columnInfo.getColumnType().getJavaTypeName());
    }
    rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, value, columnInfo);
    return (short) value;
  }

  /**
   * Get Time in string format from raw binary format.
   *
   * @param columnInfo column information
   * @return time value
   */
  public String getInternalTimeString(ColumnDefinition columnInfo) {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      // binary send 00:00:00 as 0.
      if (columnInfo.getDecimals() == 0) {
        return "00:00:00";
      } else {
        StringBuilder value = new StringBuilder("00:00:00.");
        int decimal = columnInfo.getDecimals();
        while (decimal-- > 0) {
          value.append("0");
        }
        return value.toString();
      }
    }
    String rawValue = new String(buf, pos, length, StandardCharsets.UTF_8);
    if ("0000-00-00".equals(rawValue)) {
      return null;
    }

    int day =
        ((buf[pos + 1] & 0xff)
            | ((buf[pos + 2] & 0xff) << 8)
            | ((buf[pos + 3] & 0xff) << 16)
            | ((buf[pos + 4] & 0xff) << 24));
    int hour = buf[pos + 5];
    int timeHour = hour + day * 24;

    String hourString;
    if (timeHour < 10) {
      hourString = "0" + timeHour;
    } else {
      hourString = Integer.toString(timeHour);
    }

    String minuteString;
    int minutes = buf[pos + 6];
    if (minutes < 10) {
      minuteString = "0" + minutes;
    } else {
      minuteString = Integer.toString(minutes);
    }

    String secondString;
    int seconds = buf[pos + 7];
    if (seconds < 10) {
      secondString = "0" + seconds;
    } else {
      secondString = Integer.toString(seconds);
    }

    int microseconds = 0;
    if (length > 8) {
      microseconds =
          ((buf[pos + 8] & 0xff)
              | (buf[pos + 9] & 0xff) << 8
              | (buf[pos + 10] & 0xff) << 16
              | (buf[pos + 11] & 0xff) << 24);
    }

    StringBuilder microsecondString = new StringBuilder(Integer.toString(microseconds));
    while (microsecondString.length() < 6) {
      microsecondString.insert(0, "0");
    }
    boolean negative = (buf[pos] == 0x01);
    return (negative ? "-" : "")
        + (hourString + ":" + minuteString + ":" + secondString + "." + microsecondString);
  }

  /**
   * Get BigInteger from raw binary format.
   *
   * @param columnInfo column information
   * @return BigInteger value
   * @throws SQLException if column type doesn't permit conversion or value is not in BigInteger
   *     range
   */
  public BigInteger getInternalBigInteger(ColumnDefinition columnInfo) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    switch (columnInfo.getColumnType()) {
      case BIT:
        return BigInteger.valueOf(buf[pos]);
      case TINYINT:
        return BigInteger.valueOf(columnInfo.isSigned() ? buf[pos] : (buf[pos] & 0xff));
      case SMALLINT:
      case YEAR:
        short valueShort = (short) ((buf[pos] & 0xff) | ((buf[pos + 1] & 0xff) << 8));
        return BigInteger.valueOf(columnInfo.isSigned() ? valueShort : (valueShort & 0xffff));
      case INTEGER:
      case MEDIUMINT:
        int valueInt =
            ((buf[pos] & 0xff)
                + ((buf[pos + 1] & 0xff) << 8)
                + ((buf[pos + 2] & 0xff) << 16)
                + ((buf[pos + 3] & 0xff) << 24));
        return BigInteger.valueOf(
            ((columnInfo.isSigned())
                ? valueInt
                : (valueInt >= 0) ? valueInt : valueInt & 0xffffffffL));
      case BIGINT:
        long value =
            ((buf[pos] & 0xff)
                + ((long) (buf[pos + 1] & 0xff) << 8)
                + ((long) (buf[pos + 2] & 0xff) << 16)
                + ((long) (buf[pos + 3] & 0xff) << 24)
                + ((long) (buf[pos + 4] & 0xff) << 32)
                + ((long) (buf[pos + 5] & 0xff) << 40)
                + ((long) (buf[pos + 6] & 0xff) << 48)
                + ((long) (buf[pos + 7] & 0xff) << 56));
        if (columnInfo.isSigned()) {
          return BigInteger.valueOf(value);
        } else {
          return new BigInteger(
              1,
              new byte[] {
                (byte) (value >> 56),
                (byte) (value >> 48),
                (byte) (value >> 40),
                (byte) (value >> 32),
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
              });
        }
      case FLOAT:
        return BigInteger.valueOf((long) getInternalFloat(columnInfo));
      case DOUBLE:
        return BigInteger.valueOf((long) getInternalDouble(columnInfo));
      case DECIMAL:
      case OLDDECIMAL:
        return BigInteger.valueOf(getInternalBigDecimal(columnInfo).longValue());
      default:
        return new BigInteger(new String(buf, pos, length, StandardCharsets.UTF_8));
    }
  }

  /**
   * Get ZonedDateTime from raw binary format.
   *
   * @param columnInfo column information
   * @param clazz asked class
   * @param timeZone time zone
   * @return ZonedDateTime value
   * @throws SQLException if column type doesn't permit conversion
   */
  public ZonedDateTime getInternalZonedDateTime(
      ColumnDefinition columnInfo, Class clazz, TimeZone timeZone) throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      lastValueNull |= BIT_LAST_FIELD_NULL;
      return null;
    }

    switch (columnInfo.getColumnType().getSqlType()) {
      case Types.TIMESTAMP:
        int year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);
        int month = buf[pos + 2];
        int day = buf[pos + 3];
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (length > 4) {
          hour = buf[pos + 4];
          minutes = buf[pos + 5];
          seconds = buf[pos + 6];

          if (length > 7) {
            microseconds =
                ((buf[pos + 7] & 0xff)
                    + ((buf[pos + 8] & 0xff) << 8)
                    + ((buf[pos + 9] & 0xff) << 16)
                    + ((buf[pos + 10] & 0xff) << 24));
          }
        }

        return ZonedDateTime.of(
            year,
            month,
            day,
            hour,
            minutes,
            seconds,
            microseconds * 1000,
            timeZone == null ? ZoneId.systemDefault() : timeZone.toZoneId());

      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.CHAR:

        // string conversion
        String raw = new String(buf, pos, length, StandardCharsets.UTF_8);
        if (raw.startsWith("0000-00-00 00:00:00")) {
          return null;
        }
        try {
          return ZonedDateTime.parse(raw, TEXT_ZONED_DATE_TIME);
        } catch (DateTimeParseException dateParserEx) {
          throw new SQLException(
              raw
                  + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                  + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
        }

      default:
        throw new SQLException(
            "Cannot read "
                + clazz.getName()
                + " using a "
                + columnInfo.getColumnType().getJavaTypeName()
                + " field");
    }
  }

  /**
   * Get OffsetTime from raw binary format.
   *
   * @param columnInfo column information
   * @param timeZone time zone
   * @return OffsetTime value
   * @throws SQLException if column type doesn't permit conversion
   */
  public OffsetTime getInternalOffsetTime(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      lastValueNull |= BIT_LAST_FIELD_NULL;
      return null;
    }

    ZoneId zoneId =
        timeZone == null ? ZoneId.systemDefault().normalized() : timeZone.toZoneId().normalized();
    if (zoneId instanceof ZoneOffset) {
      ZoneOffset zoneOffset = (ZoneOffset) zoneId;

      int day = 0;
      int hour = 0;
      int minutes = 0;
      int seconds = 0;
      int microseconds = 0;

      switch (columnInfo.getColumnType().getSqlType()) {
        case Types.TIMESTAMP:
          int year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);
          int month = buf[pos + 2];
          day = buf[pos + 3];

          if (length > 4) {
            hour = buf[pos + 4];
            minutes = buf[pos + 5];
            seconds = buf[pos + 6];

            if (length > 7) {
              microseconds =
                  ((buf[pos + 7] & 0xff)
                      + ((buf[pos + 8] & 0xff) << 8)
                      + ((buf[pos + 9] & 0xff) << 16)
                      + ((buf[pos + 10] & 0xff) << 24));
            }
          }

          return ZonedDateTime.of(
                  year, month, day, hour, minutes, seconds, microseconds * 1000, zoneOffset)
              .toOffsetDateTime()
              .toOffsetTime();

        case Types.TIME:
          final boolean negate = (buf[pos] & 0xff) == 0x01;

          if (length > 4) {
            day =
                ((buf[pos + 1] & 0xff)
                    + ((buf[pos + 2] & 0xff) << 8)
                    + ((buf[pos + 3] & 0xff) << 16)
                    + ((buf[pos + 4] & 0xff) << 24));
          }

          if (length > 7) {
            hour = buf[pos + 5];
            minutes = buf[pos + 6];
            seconds = buf[pos + 7];
          }

          if (length > 8) {
            microseconds =
                ((buf[pos + 8] & 0xff)
                    + ((buf[pos + 9] & 0xff) << 8)
                    + ((buf[pos + 10] & 0xff) << 16)
                    + ((buf[pos + 11] & 0xff) << 24));
          }

          return OffsetTime.of(
              (negate ? -1 : 1) * (day * 24 + hour),
              minutes,
              seconds,
              microseconds * 1000,
              zoneOffset);

        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CHAR:
          String raw = new String(buf, pos, length, StandardCharsets.UTF_8);
          try {
            return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
          } catch (DateTimeParseException dateParserEx) {
            throw new SQLException(
                raw
                    + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                    + columnInfo.getColumnType()
                    + "\")");
          }

        default:
          throw new SQLException(
              "Cannot read "
                  + OffsetTime.class.getName()
                  + " using a "
                  + columnInfo.getColumnType().getJavaTypeName()
                  + " field");
      }
    }

    if (options.useLegacyDatetimeCode) {
      // system timezone is not an offset
      throw new SQLException(
          "Cannot return an OffsetTime for a TIME field when default timezone is '"
              + zoneId
              + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
    }

    // server timezone is not an offset
    throw new SQLException(
        "Cannot return an OffsetTime for a TIME field when server timezone '"
            + zoneId
            + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
  }

  /**
   * Get LocalTime from raw binary format.
   *
   * @param columnInfo column information
   * @param timeZone time zone
   * @return LocalTime value
   * @throws SQLException if column type doesn't permit conversion
   */
  public LocalTime getInternalLocalTime(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      lastValueNull |= BIT_LAST_FIELD_NULL;
      return null;
    }

    switch (columnInfo.getColumnType().getSqlType()) {
      case Types.TIME:
        int day = 0;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        final boolean negate = (buf[pos] & 0xff) == 0x01;

        if (length > 4) {
          day =
              ((buf[pos + 1] & 0xff)
                  + ((buf[pos + 2] & 0xff) << 8)
                  + ((buf[pos + 3] & 0xff) << 16)
                  + ((buf[pos + 4] & 0xff) << 24));
        }

        if (length > 7) {
          hour = buf[pos + 5];
          minutes = buf[pos + 6];
          seconds = buf[pos + 7];
        }

        if (length > 8) {
          microseconds =
              ((buf[pos + 8] & 0xff)
                  + ((buf[pos + 9] & 0xff) << 8)
                  + ((buf[pos + 10] & 0xff) << 16)
                  + ((buf[pos + 11] & 0xff) << 24));
        }

        return LocalTime.of(
            (negate ? -1 : 1) * (day * 24 + hour), minutes, seconds, microseconds * 1000);

      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.CHAR:
        // string conversion
        String raw = new String(buf, pos, length, StandardCharsets.UTF_8);
        try {
          if (timeZone == null) {
            return LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME);
          }
          return LocalTime.parse(
              raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
        } catch (DateTimeParseException dateParserEx) {
          throw new SQLException(
              raw
                  + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                  + columnInfo.getColumnType()
                  + "\")");
        }

      case Types.TIMESTAMP:
        ZonedDateTime zonedDateTime =
            getInternalZonedDateTime(columnInfo, LocalTime.class, timeZone);
        return zonedDateTime == null
            ? null
            : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalTime();

      default:
        throw new SQLException(
            "Cannot read LocalTime using a "
                + columnInfo.getColumnType().getJavaTypeName()
                + " field");
    }
  }

  /**
   * Get LocalDate from raw binary format.
   *
   * @param columnInfo column information
   * @param timeZone time zone
   * @return LocalDate value
   * @throws SQLException if column type doesn't permit conversion
   */
  public LocalDate getInternalLocalDate(ColumnDefinition columnInfo, TimeZone timeZone)
      throws SQLException {
    if (lastValueWasNull()) {
      return null;
    }
    if (length == 0) {
      lastValueNull |= BIT_LAST_FIELD_NULL;
      return null;
    }

    switch (columnInfo.getColumnType().getSqlType()) {
      case Types.DATE:
        int year = ((buf[pos] & 0xff) | (buf[pos + 1] & 0xff) << 8);
        int month = buf[pos + 2];
        int day = buf[pos + 3];
        return LocalDate.of(year, month, day);

      case Types.TIMESTAMP:
        ZonedDateTime zonedDateTime =
            getInternalZonedDateTime(columnInfo, LocalDate.class, timeZone);
        return zonedDateTime == null
            ? null
            : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDate();

      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.CHAR:
        // string conversion
        String raw = new String(buf, pos, length, StandardCharsets.UTF_8);
        if (raw.startsWith("0000-00-00")) {
          return null;
        }
        try {
          if (timeZone == null) {
            return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE);
          }
          return LocalDate.parse(
              raw, DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
        } catch (DateTimeParseException dateParserEx) {
          throw new SQLException(
              raw + " cannot be parse as LocalDate. time must have \"yyyy-MM-dd\" format");
        }

      default:
        throw new SQLException(
            "Cannot read LocalDate using a "
                + columnInfo.getColumnType().getJavaTypeName()
                + " field");
    }
  }

  /**
   * Indicate if data is binary encoded.
   *
   * @return always true.
   */
  public boolean isBinaryEncoded() {
    return true;
  }
}
