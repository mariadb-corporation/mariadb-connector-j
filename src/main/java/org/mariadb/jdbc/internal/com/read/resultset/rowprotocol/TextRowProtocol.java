/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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
import org.mariadb.jdbc.internal.com.read.resultset.ColumnInformation;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.TimeZone;

public class TextRowProtocol extends RowProtocol {
    /**
     * Constructor.
     *
     * @param maxFieldSize  max field size
     * @param options       connection options
     */
    public TextRowProtocol(int maxFieldSize, Options options) {
        super(maxFieldSize, options);
    }

    /**
     * Set length and pos indicator to asked index.
     *
     * @param newIndex index (0 is first).
     */
    public void setPosition(int newIndex) {
        if (index != newIndex) {
            if (index == -1 || index > newIndex) {
                pos = 0;
                index = 0;
            } else {
                index++;
                if (length != NULL_LENGTH) pos += length;
            }

            for (; index <= newIndex; index++) {
                if (index != newIndex) {
                    int type = this.buf[this.pos++] & 0xff;
                    switch (type) {
                        case 251:
                            break;
                        case 252:
                            pos += 2 + (0xffff & (((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8))));
                            break;
                        case 253:
                            pos += 3 + (0xffffff & ((buf[pos] & 0xff)
                                    + ((buf[pos + 1] & 0xff) << 8)
                                    + ((buf[pos + 2] & 0xff) << 16)));
                            break;
                        case 254:
                            pos += 8 + ((buf[pos] & 0xff)
                                    + ((long) (buf[pos + 1] & 0xff) << 8)
                                    + ((long) (buf[pos + 2] & 0xff) << 16)
                                    + ((long) (buf[pos + 3] & 0xff) << 24)
                                    + ((long) (buf[pos + 4] & 0xff) << 32)
                                    + ((long) (buf[pos + 5] & 0xff) << 40)
                                    + ((long) (buf[pos + 6] & 0xff) << 48)
                                    + ((long) (buf[pos + 7] & 0xff) << 56));
                            break;
                        default:
                            pos += type;
                            break;
                    }
                } else {
                    int type = this.buf[this.pos++] & 0xff;
                    switch (type) {
                        case 251:
                            length = NULL_LENGTH;
                            this.lastValueNull = BIT_LAST_FIELD_NULL;
                            return;
                        case 252:
                            length = 0xffff & ((buf[pos++] & 0xff)
                                    + ((buf[pos++] & 0xff) << 8));
                            break;
                        case 253:
                            length = 0xffffff & ((buf[pos++] & 0xff)
                                    + ((buf[pos++] & 0xff) << 8)
                                    + ((buf[pos++] & 0xff) << 16));
                            break;
                        case 254:
                            length = (int) ((buf[pos++] & 0xff)
                                    + ((long) (buf[pos++] & 0xff) << 8)
                                    + ((long) (buf[pos++] & 0xff) << 16)
                                    + ((long) (buf[pos++] & 0xff) << 24)
                                    + ((long) (buf[pos++] & 0xff) << 32)
                                    + ((long) (buf[pos++] & 0xff) << 40)
                                    + ((long) (buf[pos++] & 0xff) << 48)
                                    + ((long) (buf[pos++] & 0xff) << 56));
                            break;
                        default:
                            length = type;
                            break;
                    }
                    this.lastValueNull = BIT_LAST_FIELD_NOT_NULL;
                    return;
                }
            }
        }
        this.lastValueNull = length == NULL_LENGTH ? BIT_LAST_FIELD_NULL : BIT_LAST_FIELD_NOT_NULL;
    }

    /**
     * Get String from raw text format.
     *
     * @param columnInfo    column information
     * @param cal           calendar
     * @param timeZone      time zone
     * @return String value
     * @throws SQLException if column type doesn't permit conversion
     */
    public String getInternalString(ColumnInformation columnInfo, Calendar cal, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;

        switch (columnInfo.getColumnType()) {
            case BIT:
                return String.valueOf(parseBit());
            case DOUBLE:
                return zeroFillingIfNeeded(String.valueOf(getInternalDouble(columnInfo)), columnInfo);
            case FLOAT:
                return zeroFillingIfNeeded(String.valueOf(getInternalFloat(columnInfo)), columnInfo);
            case TIME:
                return getInternalTimeString(columnInfo);
            case DATE:
                Date date = getInternalDate(columnInfo, cal, timeZone);
                if (date == null) {
                    if ((lastValueNull & BIT_LAST_ZERO_DATE) != 0) {
                        lastValueNull ^= BIT_LAST_ZERO_DATE;
                        return new String(buf, pos, length, StandardCharsets.UTF_8);
                    }
                    return null;
                }
                return date.toString();
            case YEAR:
                if (options.yearIsDateType) {
                    Date date1 = getInternalDate(columnInfo, cal, timeZone);
                    return (date1 == null) ? null : date1.toString();
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal, timeZone);
                if (timestamp == null) {
                    if ((lastValueNull & BIT_LAST_ZERO_DATE) != 0) {
                        lastValueNull ^= BIT_LAST_ZERO_DATE;
                        return new String(buf, pos, length, StandardCharsets.UTF_8);
                    }
                    return null;
                }
                return timestamp.toString();
            case DECIMAL:
            case OLDDECIMAL:
                BigDecimal bigDecimal = getInternalBigDecimal(columnInfo);
                return (bigDecimal == null) ? null : zeroFillingIfNeeded(bigDecimal.toString(), columnInfo);
            case NULL:
                return null;
            default:
                break;
        }

        if (maxFieldSize > 0) {
            return new String(buf, pos, Math.max(maxFieldSize * 3, length), StandardCharsets.UTF_8)
                    .substring(0, maxFieldSize);
        }

        return new String(buf, pos, length, StandardCharsets.UTF_8);
    }

    /**
     * Get int from raw text format.
     *
     * @param columnInfo    column information
     * @return int value
     * @throws SQLException if column type doesn't permit conversion or not in Integer range
     */
    public int getInternalInt(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        long value = getInternalLong(columnInfo);
        rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value, columnInfo);
        return (int) value;
    }

    /**
     * Get long from raw text format.
     *
     * @param columnInfo    column information
     * @return long value
     * @throws SQLException if column type doesn't permit conversion or not in Long range (unsigned)
     */
    public long getInternalLong(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        try {
            switch (columnInfo.getColumnType()) {
                case FLOAT:
                    Float floatValue = Float.valueOf(new String(buf, pos, length, StandardCharsets.UTF_8));
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(buf, pos, length, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(new String(buf, pos, length, StandardCharsets.UTF_8));
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + new String(buf, pos, length, StandardCharsets.UTF_8)
                                + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                case BIT:
                    return parseBit();
                case TINYINT:
                case SMALLINT:
                case YEAR:
                case INTEGER:
                case MEDIUMINT:
                case BIGINT:
                    long result = 0;
                    boolean negate = false;
                    int begin = pos;
                    if (length > 0 && buf[begin] == 45) { //minus sign
                        negate = true;
                        begin++;
                    }
                    for (; begin < pos + length; begin++) {
                        result = result * 10 + buf[begin] - 48;
                    }
                    //specific for BIGINT : if value > Long.MAX_VALUE , will become negative until -1
                    if (result < 0) {
                        //CONJ-399 : handle specifically Long.MIN_VALUE that has absolute value +1 compare to LONG.MAX_VALUE
                        if (result == Long.MIN_VALUE && negate) return Long.MIN_VALUE;
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' for value "
                                + new String(buf, pos, length, StandardCharsets.UTF_8), "22003", 1264);
                    }
                    return (negate ? -1 * result : result);
                default:
                    return Long.parseLong(new String(buf, pos, length, StandardCharsets.UTF_8));
            }

        } catch (NumberFormatException nfe) {
            //parse error.
            //if its a decimal retry without the decimal part.
            String value = new String(buf, pos, length, StandardCharsets.UTF_8);
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Long.parseLong(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException nfee) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value, "22003", 1264);
        }
    }

    /**
     * Get float from raw text format.
     *
     * @param columnInfo    column information
     * @return float value
     * @throws SQLException if column type doesn't permit conversion or not in Float range
     */
    public float getInternalFloat(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;

        switch (columnInfo.getColumnType()) {
            case BIT:
                return parseBit();
            case TINYINT:
            case SMALLINT:
            case YEAR:
            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case VARSTRING:
            case VARCHAR:
            case STRING:
            case OLDDECIMAL:
            case BIGINT:
                try {
                    return Float.valueOf(new String(buf, pos, length, StandardCharsets.UTF_8));
                } catch (NumberFormatException nfe) {
                    SQLException sqlException = new SQLException("Incorrect format \""
                            + new String(buf, pos, length, StandardCharsets.UTF_8)
                            + "\" for getFloat for data field with type " + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                    //noinspection UnnecessaryInitCause
                    sqlException.initCause(nfe);
                    throw sqlException;
                }
            default:
                throw new SQLException("getFloat not available for data field type " + columnInfo.getColumnType().getJavaTypeName());
        }
    }

    /**
     * Get double from raw text format.
     *
     * @param columnInfo    column information
     * @return double value
     * @throws SQLException if column type doesn't permit conversion or not in Double range (unsigned)
     */
    public double getInternalDouble(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        switch (columnInfo.getColumnType()) {
            case BIT:
                return parseBit();
            case TINYINT:
            case SMALLINT:
            case YEAR:
            case INTEGER:
            case MEDIUMINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case VARSTRING:
            case VARCHAR:
            case STRING:
            case OLDDECIMAL:
            case BIGINT:
                try {
                    return Double.valueOf(new String(buf, pos, length, StandardCharsets.UTF_8));
                } catch (NumberFormatException nfe) {
                    SQLException sqlException = new SQLException("Incorrect format \""
                            + new String(buf, pos, length, StandardCharsets.UTF_8)
                            + "\" for getDouble for data field with type " + columnInfo.getColumnType().getJavaTypeName(), "22003", 1264);
                    //noinspection UnnecessaryInitCause
                    sqlException.initCause(nfe);
                    throw sqlException;
                }
            default:
                throw new SQLException("getDouble not available for data field type " + columnInfo.getColumnType().getJavaTypeName());
        }
            
    }

    /**
     * Get BigDecimal from raw text format.
     *
     * @param columnInfo    column information
     * @return BigDecimal value
     * @throws SQLException if column type doesn't permit conversion
     */
    public BigDecimal getInternalBigDecimal(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return null;

        if (columnInfo.getColumnType() == ColumnType.BIT) {
            return BigDecimal.valueOf(parseBit());
        }
        return new BigDecimal(new String(buf, pos, length, StandardCharsets.UTF_8));
    }

    /**
     * Get date from raw text format.
     *
     * @param columnInfo    column information
     * @param cal           calendar
     * @param timeZone      time zone
     * @return date value
     * @throws SQLException if column type doesn't permit conversion
     */
    @SuppressWarnings( "deprecation" )
    public Date getInternalDate(ColumnInformation columnInfo, Calendar cal, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;

        String rawValue = new String(buf, pos, length, StandardCharsets.UTF_8);
        switch (columnInfo.getColumnType()) {
            case TIMESTAMP:
            case DATETIME:
                Timestamp timestamp = getInternalTimestamp(columnInfo, cal, timeZone);
                if (timestamp == null) return null;
                return new Date(timestamp.getTime());

            case TIME:
                throw new SQLException("Cannot read DATE using a Types.TIME field");

            case DATE:
                if ("0000-00-00".equals(rawValue)) {
                    lastValueNull |= BIT_LAST_ZERO_DATE;
                    return null;
                }

                return new Date(
                        Integer.parseInt(rawValue.substring(0, 4)) - 1900,
                        Integer.parseInt(rawValue.substring(5, 7)) - 1,
                        Integer.parseInt(rawValue.substring(8, 10))
                );

            case YEAR:
                int year = Integer.parseInt(rawValue);
                if (length == 2 && columnInfo.getLength() == 2) {
                    if (year <= 69) {
                        year += 2000;
                    } else {
                        year += 1900;
                    }
                }

                return new Date(year - 1900, 0, 1);

            default:

                try {

                    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    sdf.setTimeZone(timeZone);
                    java.util.Date utilDate = sdf.parse(rawValue);
                    return new Date(utilDate.getTime());

                } catch (ParseException e) {
                    throw ExceptionMapper.getSqlException("Could not get object as Date : " + e.getMessage(), "S1009", e);
                }
        }
    }

    /**
     * Get time from raw text format.
     *
     * @param columnInfo    column information
     * @param cal           calendar
     * @param timeZone      time zone
     * @return time value
     * @throws SQLException if column type doesn't permit conversion
     */
    public Time getInternalTime(ColumnInformation columnInfo, Calendar cal, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;

        if (columnInfo.getColumnType() == ColumnType.TIMESTAMP || columnInfo.getColumnType() == ColumnType.DATETIME) {
            Timestamp timestamp = getInternalTimestamp(columnInfo, cal, timeZone);
            return (timestamp == null) ? null : new Time(timestamp.getTime());

        } else if (columnInfo.getColumnType() == ColumnType.DATE) {

            throw new SQLException("Cannot read Time using a Types.DATE field");

        } else {
            String raw = new String(buf, pos, length, StandardCharsets.UTF_8);
            if (!options.useLegacyDatetimeCode && (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3)) {
                throw new SQLException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss");
            }
            boolean negate = raw.startsWith("-");
            if (negate) {
                raw = raw.substring(1);
            }
            String[] rawPart = raw.split(":");
            if (rawPart.length == 3) {
                int hour = Integer.parseInt(rawPart[0]);
                int minutes = Integer.parseInt(rawPart[1]);
                int seconds = Integer.parseInt(rawPart[2].substring(0, 2));
                Calendar calendar = Calendar.getInstance();
                if (options.useLegacyDatetimeCode) {
                    calendar.setLenient(true);
                }
                calendar.clear();
                calendar.set(1970, Calendar.JANUARY, 1, (negate ? -1 : 1) * hour, minutes, seconds);
                int nanoseconds = extractNanos(raw);
                calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);

                return new Time(calendar.getTimeInMillis());
            } else {
                throw new SQLException(raw + " cannot be parse as time. time must have \"99:99:99\" format");
            }
        }
   
    }

    /**
     * Get timestamp from raw text format.
     *
     * @param columnInfo    column information
     * @param userCalendar  calendar
     * @param timeZone      time zone
     * @return timestamp value
     * @throws SQLException if column type doesn't permit conversion
     */
    public Timestamp getInternalTimestamp(ColumnInformation columnInfo, Calendar userCalendar, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;

        String rawValue = new String(buf, pos, length, StandardCharsets.UTF_8);
        if (rawValue.startsWith("0000-00-00 00:00:00")) {
            lastValueNull |= BIT_LAST_ZERO_DATE;
            return null;
        }

        switch (columnInfo.getColumnType()) {
            case TIME:
                //time does not go after millisecond
                Timestamp tt = new Timestamp(getInternalTime(columnInfo, userCalendar, timeZone).getTime());
                tt.setNanos(extractNanos(rawValue));
                return tt;
            default:
                try {
                    int hour = 0;
                    int minutes = 0;
                    int seconds = 0;

                    int year = Integer.parseInt(rawValue.substring(0, 4));
                    int month = Integer.parseInt(rawValue.substring(5, 7));
                    int day = Integer.parseInt(rawValue.substring(8, 10));
                    if (rawValue.length() >= 19) {
                        hour = Integer.parseInt(rawValue.substring(11, 13));
                        minutes = Integer.parseInt(rawValue.substring(14, 16));
                        seconds = Integer.parseInt(rawValue.substring(17, 19));
                    }
                    int nanoseconds = extractNanos(rawValue);
                    Timestamp timestamp;

                    Calendar calendar;
                    if (userCalendar != null) {
                        calendar = userCalendar;
                    } else if (columnInfo.getColumnType().getSqlType() == Types.TIMESTAMP) {
                        calendar = Calendar.getInstance(timeZone);
                    } else {
                        calendar = Calendar.getInstance();
                    }

                    synchronized (calendar) {
                        calendar.clear();
                        calendar.set(Calendar.YEAR, year);
                        calendar.set(Calendar.MONTH, month - 1);
                        calendar.set(Calendar.DAY_OF_MONTH, day);
                        calendar.set(Calendar.HOUR_OF_DAY, hour);
                        calendar.set(Calendar.MINUTE, minutes);
                        calendar.set(Calendar.SECOND, seconds);
                        calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);
                        timestamp = new Timestamp(calendar.getTime().getTime());
                    }
                    timestamp.setNanos(nanoseconds);
                    return timestamp;
                } catch (NumberFormatException | StringIndexOutOfBoundsException n) {
                    throw new SQLException("Value \"" + rawValue + "\" cannot be parse as Timestamp");
                }
        }
    }

    /**
     * Get Object from raw text format.
     *
     * @param columnInfo            column information
     * @param dataTypeMappingFlags  hint to indicate how to handle YEAR and TINYINT
     * @param timeZone              time zone
     * @return Object value
     * @throws SQLException if column type doesn't permit conversion
     */
    public Object getInternalObject(ColumnInformation columnInfo, int dataTypeMappingFlags, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;

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
                    return buf[pos] != '0';
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
                if ((dataTypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
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
            case VARSTRING:
            case STRING:
                if (columnInfo.isBinary()) {
                    byte[] data = new byte[getLengthMaxFieldSize()];
                    System.arraycopy(buf, pos, data, 0, getLengthMaxFieldSize());
                    return data;
                }
                return getInternalString(columnInfo, null, timeZone);
            case OLDDECIMAL:
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
        throw ExceptionMapper.getFeatureNotSupportedException("Type '" + columnInfo.getColumnType().getTypeName() + "' is not supported");
    }

    /**
     * Get boolean from raw text format.
     *
     * @param columnInfo    column information
     * @return boolean value
     * @throws SQLException if column type doesn't permit conversion
     */
    public boolean getInternalBoolean(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return false;

        if (columnInfo.getColumnType() == ColumnType.BIT) return parseBit() != 0;
        final String rawVal = new String(buf, pos, length, StandardCharsets.UTF_8);
        return !("false".equals(rawVal) || "0".equals(rawVal));
    }

    /**
     * Get byte from raw text format.
     *
     * @param columnInfo    column information
     * @return byte value
     * @throws SQLException if column type doesn't permit conversion
     */
    public byte getInternalByte(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        long value = getInternalLong(columnInfo);
        rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, value, columnInfo);
        return (byte) value;
    }

    /**
     * Get short from raw text format.
     *
     * @param columnInfo    column information
     * @return short value
     * @throws SQLException if column type doesn't permit conversion or value is not in Short range
     */
    public short getInternalShort(ColumnInformation columnInfo) throws SQLException {
        if (lastValueWasNull()) return 0;
        long value = getInternalLong(columnInfo);
        rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, value, columnInfo);
        return (short) value;        
    }

    /**
     * Get Time in string format from raw text format.
     *
     * @param columnInfo    column information
     * @return String representation of time
     */
    public String getInternalTimeString(ColumnInformation columnInfo) {
        if (lastValueWasNull()) return null;

        String rawValue = new String(buf, pos, length, StandardCharsets.UTF_8);
        if ("0000-00-00".equals(rawValue)) return null;

        if (options.maximizeMysqlCompatibility && options.useLegacyDatetimeCode && rawValue.indexOf(".") > 0) {
            return rawValue.substring(0, rawValue.indexOf("."));
        }
        return rawValue;
    }

    /**
     * Get BigInteger format from raw text format.
     *
     * @param columnInfo    column information
     * @return BigInteger value
     */
    public BigInteger getInternalBigInteger(ColumnInformation columnInfo) {
        if (lastValueWasNull()) return null;
        return new BigInteger(new String(buf, pos, length, StandardCharsets.UTF_8));
    }

    /**
     * Get ZonedDateTime format from raw text format.
     *
     * @param columnInfo    column information
     * @param clazz         class for logging
     * @param timeZone      time zone
     * @return ZonedDateTime value
     * @throws SQLException if column type doesn't permit conversion
     */
    public ZonedDateTime getInternalZonedDateTime(ColumnInformation columnInfo, Class clazz, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        String raw = new String(buf, pos, length, StandardCharsets.UTF_8);

        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.TIMESTAMP:

                if (raw.startsWith("0000-00-00 00:00:00")) return null;
                try {
                    LocalDateTime localDateTime = LocalDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(timeZone.toZoneId()));
                    return ZonedDateTime.of(localDateTime, timeZone.toZoneId());
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(raw + " cannot be parse as LocalDateTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                }

            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:

                if (raw.startsWith("0000-00-00 00:00:00")) return null;
                try {
                    return ZonedDateTime.parse(raw, TEXT_ZONED_DATE_TIME);
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(raw + " cannot be parse as ZonedDateTime. time must have \"yyyy-MM-dd[T/ ]HH:mm:ss[.S]\" "
                            + "with offset and timezone format (example : '2011-12-03 10:15:30+01:00[Europe/Paris]')");
                }

            default:
                throw new SQLException("Cannot read " + clazz.getName() + " using a " + columnInfo.getColumnType().getJavaTypeName() + " field");

        }

    }

    /**
     * Get OffsetTime format from raw text format.
     *
     * @param columnInfo    column information
     * @param timeZone      time zone
     * @return OffsetTime value
     * @throws SQLException if column type doesn't permit conversion
     */
    public OffsetTime getInternalOffsetTime(ColumnInformation columnInfo, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        ZoneId zoneId = timeZone.toZoneId().normalized();
        if (ZoneOffset.class.isInstance(zoneId)) {
            ZoneOffset zoneOffset = ZoneOffset.class.cast(zoneId);
            String raw = new String(buf, pos, length, StandardCharsets.UTF_8);
            switch (columnInfo.getColumnType().getSqlType()) {

                case Types.TIMESTAMP:
                    if (raw.startsWith("0000-00-00 00:00:00")) return null;
                    try {
                        return ZonedDateTime.parse(raw, TEXT_LOCAL_DATE_TIME.withZone(zoneOffset)).toOffsetDateTime().toOffsetTime();
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as OffsetTime. time must have \"yyyy-MM-dd HH:mm:ss[.S]\" format");
                    }

                case Types.TIME:
                    try {
                        LocalTime localTime = LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(zoneOffset));
                        return OffsetTime.of(localTime, zoneOffset);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CHAR:
                    try {
                        return OffsetTime.parse(raw, DateTimeFormatter.ISO_OFFSET_TIME);
                    } catch (DateTimeParseException dateParserEx) {
                        throw new SQLException(raw + " cannot be parse as OffsetTime (format is \"HH:mm:ss[.S]\" with offset for data type \""
                                + columnInfo.getColumnType() + "\")");
                    }

                default:
                    throw new SQLException("Cannot read " + OffsetTime.class.getName() + " using a "
                            + columnInfo.getColumnType().getJavaTypeName() + " field");
            }
        }

        if (options.useLegacyDatetimeCode) {
            //system timezone is not an offset
            throw new SQLException("Cannot return an OffsetTime for a TIME field when default timezone is '" + zoneId
                    + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
        }

        //server timezone is not an offset
        throw new SQLException("Cannot return an OffsetTime for a TIME field when server timezone '" + zoneId
                + "' (only possible for time-zone offset from Greenwich/UTC, such as +02:00)");
        
    }

    /**
     * Get LocalTime format from raw text format.
     *
     * @param columnInfo    column information
     * @param timeZone      time zone
     * @return LocalTime value
     * @throws SQLException if column type doesn't permit conversion
     */
    public LocalTime getInternalLocalTime(ColumnInformation columnInfo, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        String raw = new String(buf, pos, length, StandardCharsets.UTF_8);

        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.TIME:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:
                try {
                    return LocalTime.parse(raw, DateTimeFormatter.ISO_LOCAL_TIME.withZone(timeZone.toZoneId()));
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(raw + " cannot be parse as LocalTime (format is \"HH:mm:ss[.S]\" for data type \""
                            + columnInfo.getColumnType() + "\")");
                }

            case Types.TIMESTAMP:
                ZonedDateTime zonedDateTime = getInternalZonedDateTime(columnInfo, LocalTime.class, timeZone);
                return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalTime();

            default:
                throw new SQLException("Cannot read LocalTime using a " + columnInfo.getColumnType().getJavaTypeName() + " field");
        }

    }

    /**
     * Get LocalDate format from raw text format.
     *
     * @param columnInfo    column information
     * @param timeZone      time zone
     * @return LocalDate value
     * @throws SQLException if column type doesn't permit conversion
     */
    public LocalDate getInternalLocalDate(ColumnInformation columnInfo, TimeZone timeZone) throws SQLException {
        if (lastValueWasNull()) return null;
        if (length == 0) {
            lastValueNull |= BIT_LAST_FIELD_NULL;
            return null;
        }

        String raw = new String(buf, pos, length, StandardCharsets.UTF_8);

        switch (columnInfo.getColumnType().getSqlType()) {
            case Types.DATE:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CHAR:
                if (raw.startsWith("0000-00-00")) return null;
                try {
                    return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE.withZone(timeZone.toZoneId()));
                } catch (DateTimeParseException dateParserEx) {
                    throw new SQLException(raw + " cannot be parse as LocalDate (format is \"yyyy-MM-dd\" for data type \""
                            + columnInfo.getColumnType() + "\")");
                }

            case Types.TIMESTAMP:
                ZonedDateTime zonedDateTime = getInternalZonedDateTime(columnInfo, LocalDate.class, timeZone);
                return zonedDateTime == null ? null : zonedDateTime.withZoneSameInstant(ZoneId.systemDefault()).toLocalDate();

            default:
                throw new SQLException("Cannot read LocalDate using a " + columnInfo.getColumnType().getJavaTypeName() + " field");

        }
    }

    /**
     * Indicate if data is binary encoded.
     * @return always false.
     */
    public boolean isBinaryEncoded() {
        return false;
    }

}