/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.mysql;

import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.internal.common.Options;
import org.mariadb.jdbc.internal.common.ValueObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Contains the raw value returned from the server.
 * <p>
 * Is immutable
 */
public class MariaDbValueObject implements ValueObject {
    private final byte[] rawBytes;
    private final MariaDbType dataType;
    private final boolean isBinaryEncoded;
    private final ColumnInformation columnInfo;
    private final Options options;

    /**
     * Constructor.
     * @param rawBytes raw data
     * @param columnInfo column information
     * @param options session options
     */
    public MariaDbValueObject(byte[] rawBytes, ColumnInformation columnInfo, Options options) {
        this.dataType = columnInfo.getType();
        this.rawBytes = rawBytes;
        this.isBinaryEncoded = false;
        this.columnInfo = columnInfo;
        this.options = options;
    }

    /**
     * Constructor.
     * @param rawBytes raw data
     * @param columnInfo column information
     * @param isBinaryEncoded is text or binary encoded.
     * @param options session options
     */
    public MariaDbValueObject(byte[] rawBytes, ColumnInformation columnInfo, boolean isBinaryEncoded, Options options) {
        this.dataType = columnInfo.getType();
        this.rawBytes = rawBytes;
        this.isBinaryEncoded = isBinaryEncoded;
        this.columnInfo = columnInfo;
        this.options = options;
    }


    public String getString() {
        return getString(null);
    }

    /**
     * Get String from raw data.
     * @param cal session calendar
     * @return string
     */
    public String getString(Calendar cal) {
        if (rawBytes == null) {
            return null;
        }

        switch (columnInfo.getType()) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return (rawBytes[0] == 0) ? "0" : "1";
                }
                break;
            case TIME:
                return getTimeString();
            case DATE:
                if (isBinaryEncoded) {
                    try {
                        return getDate(cal).toString();
                    } catch (ParseException e) {
                    }
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                if (isBinaryEncoded) {
                    try {
                        return getTimestamp(cal).toString();
                    } catch (ParseException e) {
                    }
                }
                break;
            default:
                break;
        }
        return new String(rawBytes, StandardCharsets.UTF_8);
    }

    private String getTimeString() {
        if (rawBytes == null) {
            return null;
        }

        String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
        String zeroDate = "0000-00-00";
        if (rawValue.equals(zeroDate)) {
            return null;
        }

        if (!this.isBinaryEncoded) {
            if (options.useLegacyDatetimeCode) {
                return rawValue;
            }
            if (rawValue.indexOf(".") > 0) {
                return rawValue.substring(0, rawValue.indexOf("."));
            }
            return rawValue;
        }

        if (rawBytes.length == 0) {
            return null;
        }

        int day = ((rawBytes[1] & 0xff)
                | ((rawBytes[2] & 0xff) << 8)
                | ((rawBytes[3] & 0xff) << 16)
                | ((rawBytes[4] & 0xff) << 24));
        int hour = rawBytes[5];
        int timeHour = hour + day * 24;

        String hourString;

        if (timeHour < 10) {
            hourString = "0" + timeHour;
        } else {
            hourString = Integer.toString(timeHour);
        }
        String minuteString;
        int minutes = rawBytes[6];
        if (minutes < 10) {
            minuteString = "0" + minutes;
        } else {
            minuteString = Integer.toString(minutes);
        }
        String secondString;
        int seconds = rawBytes[7];
        if (seconds < 10) {
            secondString = "0" + seconds;
        } else {
            secondString = Integer.toString(seconds);
        }
        int microseconds = 0;
        if (rawBytes.length > 8) {
            microseconds = ((rawBytes[8] & 0xff)
                    | (rawBytes[9] & 0xff) << 8
                    | (rawBytes[10] & 0xff) << 16
                    | (rawBytes[11] & 0xff) << 24);
        }
        String microsecondString = Integer.toString(microseconds);
        while (microsecondString.length() < 6) {
            microsecondString = "0" + microsecondString;
        }
        boolean negative = (rawBytes[0] == 0x01);
        return (negative ? "-" : "") + (hourString + ":" + minuteString + ":" + secondString);
    }

    /**
     * Get byte from raw data.
     * @return byte
     */
    public byte getByte() {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            if (dataType == MariaDbType.BIT) {
                return rawBytes[0];
            }
            try {
                return Byte.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal value = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (value.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0) {
                    return Byte.MIN_VALUE;
                }
                if (value.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0) {
                    return Byte.MAX_VALUE;
                }
                return value.byteValue();
            }
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    if (columnInfo.isSigned()) {
                        return rawBytes[0];
                    } else {
                        return (byte) (rawBytes[0] & 0xff);
                    }
                case SMALLINT:
                case YEAR:
                    return (byte) getShort();
                case INTEGER:
                case MEDIUMINT:
                    return (byte) getInt();
                case BIGINT:
                    return (byte) getLong();
                case FLOAT:
                    return (byte) getFloat();
                case DOUBLE:
                    return (byte) getDouble();
                default:
                    try {
                        return Byte.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        BigDecimal value = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (value.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0) {
                            return Byte.MIN_VALUE;
                        }
                        if (value.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0) {
                            return Byte.MAX_VALUE;
                        }
                        return value.byteValue();
                    }
            }
        }
    }

    /**
     * Get short from raw data.
     * @return short
     */
    public short getShort() {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            try {
                return Short.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal value = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (value.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0) {
                    return Short.MIN_VALUE;
                }
                if (value.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0) {
                    return Short.MAX_VALUE;
                }
                return value.shortValue();
            }
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getByte();
                case SMALLINT:
                case YEAR:
                    short value = (short) ((rawBytes[0] & 0xff) | ((rawBytes[1] & 0xff) << 8));
                    if (columnInfo.isSigned()) {
                        return value;
                    } else {
                        return (short) (value & 0xffff);
                    }
                case INTEGER:
                case MEDIUMINT:
                    return (short) getInt();
                case BIGINT:
                    return (short) getLong();
                case FLOAT:
                    return (short) getFloat();
                case DOUBLE:
                    return (short) getDouble();
                default:
                    try {
                        return Short.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        BigDecimal bigdecimal = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (bigdecimal.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0) {
                            return Short.MIN_VALUE;
                        }
                        if (bigdecimal.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0) {
                            return Short.MAX_VALUE;
                        }
                        return bigdecimal.shortValue();
                    }
            }
        }
    }

    /**
     * Get int from raw data.
     * @return int
     */
    public int getInt() {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            try {
                return Integer.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal value = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (value.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
                    return Integer.MIN_VALUE;
                }
                if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                    return Integer.MAX_VALUE;
                }
                return value.intValue();
            }
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getByte();
                case SMALLINT:
                case YEAR:
                    return getShort();
                case INTEGER:
                case MEDIUMINT:
                    int value = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    if (columnInfo.isSigned()) {
                        return value;
                    } else {
                        return (value & 0xffffffff);
                    }
                case BIGINT:
                    return (int) getLong();
                case FLOAT:
                    return (int) getFloat();
                case DOUBLE:
                    return (int) getDouble();
                default:
                    try {
                        return Integer.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        BigDecimal bigdecimal = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (bigdecimal.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0) {
                            return Integer.MIN_VALUE;
                        }
                        if (bigdecimal.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                            return Integer.MAX_VALUE;
                        }
                        return bigdecimal.intValue();
                    }
            }
        }
    }

    /**
     * Get long from raw data.
     * @return long
     */
    public long getLong() {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            try {
                return Long.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal bigdecimal = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (bigdecimal.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0) {
                    return Long.MIN_VALUE;
                }
                if (bigdecimal.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                    return Long.MAX_VALUE;
                }
                return bigdecimal.longValue();
            }
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getByte();
                case SMALLINT:
                case YEAR:
                    return getShort();
                case INTEGER:
                case MEDIUMINT:
                    return getInt();
                case BIGINT:
                    long value = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return value;
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) (value >> 0)}).longValue();
                    }
                case FLOAT:
                    return (long) getFloat();
                case DOUBLE:
                    return (long) getDouble();
                default:
                    try {
                        return Long.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        BigDecimal bigdecimal = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (bigdecimal.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0) {
                            return Long.MIN_VALUE;
                        }
                        if (bigdecimal.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                            return Long.MAX_VALUE;
                        }
                        return bigdecimal.longValue();
                    }
            }
        }
    }

    /**
     * Get float from raw data.
     * @return float
     */
    public float getFloat() {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getByte();
                case SMALLINT:
                case YEAR:
                    return getShort();
                case INTEGER:
                case MEDIUMINT:
                    return getInt();
                case BIGINT:
                    return getLong();
                case FLOAT:
                    int value = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    return Float.intBitsToFloat(value);
                case DOUBLE:
                    return (float) getDouble();
                default:
                    return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Get double value from raw data.
     * @return double
     */
    public double getDouble() {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getByte();
                case SMALLINT:
                case YEAR:
                    return getShort();
                case INTEGER:
                case MEDIUMINT:
                    return getInt();
                case BIGINT:
                    return getLong();
                case FLOAT:
                    return getFloat();
                case DOUBLE:
                    long value = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56));
                    return Double.longBitsToDouble(value);
                default:
                    return Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Get BigDecimal from rax data.
     * @return Bigdecimal value
     */
    public BigDecimal getBigDecimal() {
        if (rawBytes == null) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            return new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (dataType) {
                case BIT:
                    return BigDecimal.valueOf((long) rawBytes[0]);
                case TINYINT:
                    return BigDecimal.valueOf((long) getByte());
                case SMALLINT:
                case YEAR:
                    return BigDecimal.valueOf(getShort());
                case INTEGER:
                case MEDIUMINT:
                    return BigDecimal.valueOf((long) getInt());
                case BIGINT:
                    long value = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return new BigDecimal(String.valueOf(BigInteger.valueOf(value))).setScale(columnInfo.getDecimals());
                    } else {
                        return new BigDecimal(String.valueOf(new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) (value >> 0)}))).setScale(columnInfo.getDecimals());
                    }
                case FLOAT:
                    return BigDecimal.valueOf((long) getFloat());
                case DOUBLE:
                    return BigDecimal.valueOf((long) getDouble());
                default:
                    return new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }

    }

    public byte[] getBytes() {
        return rawBytes;
    }

    /**
     * Get BigInteger from raw data.
     * @return bigInteger
     */
    public BigInteger getBigInteger() {
        if (rawBytes == null) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            return new BigInteger(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            switch (dataType) {
                case BIT:
                    return BigInteger.valueOf((long) rawBytes[0]);
                case TINYINT:
                    return BigInteger.valueOf((long) getByte());
                case SMALLINT:
                case YEAR:
                    return BigInteger.valueOf(getShort());
                case INTEGER:
                case MEDIUMINT:
                    return BigInteger.valueOf((long) getInt());
                case BIGINT:
                    long value = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return BigInteger.valueOf(value);
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (value >> 56),
                                (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                                (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                                (byte) (value >> 0)});
                    }
                case FLOAT:
                    return BigInteger.valueOf((long) getFloat());
                case DOUBLE:
                    return BigInteger.valueOf((long) getDouble());
                default:
                    return new BigInteger(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }

    }

    /**
     * Get date from raw data.
     * @param cal session calendar
     * @return date
     * @throws ParseException if raw data cannot be parse
     */
    public Date getDate(Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }

        if (!this.isBinaryEncoded) {
            String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
            String zeroDate = "0000-00-00";

            if (rawValue.equals(zeroDate)) {
                return null;
            }

            SimpleDateFormat sdf;
            switch (dataType) {
                case TIMESTAMP:
                case DATETIME:
                    return new Date(getTimestamp(cal).getTime());
                case TIME:
                    return new Date(getTime(cal).getTime());
                case DATE:
                    return new Date(
                            Integer.parseInt(rawValue.substring(0, 4)) - 1900,
                            Integer.parseInt(rawValue.substring(5, 7)) - 1,
                            Integer.parseInt(rawValue.substring(8, 10))
                    );
                case YEAR:
                    int year = Integer.parseInt(rawValue);
                    if (rawBytes.length == 2) {
                        if (columnInfo.getLength() == 2) {
                            if (year <= 69) {
                                year += 2000;
                            } else {
                                year += 1900;
                            }
                        }
                    }

                    return new Date(year - 1900, 0, 1);
                default:
                    sdf = new SimpleDateFormat("yyyy-MM-dd");
                    if (cal != null) {
                        sdf.setCalendar(cal);
                    }
            }
            java.util.Date utilDate = sdf.parse(rawValue);
            return new Date(utilDate.getTime());
        } else {
            return binaryDate(cal);
        }
    }

    /**
     * Get time from raw data.
     * @param cal session calendar
     * @return time value
     * @throws ParseException if raw data cannot be parse
     */
    public Time getTime(Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        String rawValueSt = new String(rawBytes, StandardCharsets.UTF_8);
        String zeroDate = "0000-00-00";
        if (rawValueSt.equals(zeroDate)) {
            return null;
        }

        if (!this.isBinaryEncoded) {
            String raw = new String(rawBytes, StandardCharsets.UTF_8);
            switch (dataType) {
                case TIMESTAMP:
                case DATETIME:
                    return new Time(getTimestamp(cal).getTime());
                default:
                    if (!options.useLegacyDatetimeCode) {
                        //time stored without server timezone
                        if (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3) {
                            throw new ParseException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss", 0);
                        }
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
                        calendar.set(1970, 0, 1, (negate ? -1 : 1) * hour, minutes, seconds);
                        int nanoseconds = extractNanos(raw);
                        calendar.set(Calendar.MILLISECOND, nanoseconds / 1000000);

                        return new Time(calendar.getTimeInMillis());
                    } else {
                        throw new ParseException(raw + " cannot be parse as time. time must have \"99:99:99\" format", 0);
                    }
            }
        } else {
            return binaryTime();
        }
    }

    private Date binaryDate(Calendar cal) throws ParseException {
        if (rawBytes.length == 0) {
            return null;
        }
        int year = 1970;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;

        year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
        month = rawBytes[2];
        day = rawBytes[3];

        Calendar calendar = Calendar.getInstance();
        /*if (!options.useLegacyDatetimeCode) {
            c = cal;
        }*/

        Date dt;
        synchronized (calendar) {
            calendar.clear();
            calendar.set(Calendar.YEAR, year);
            calendar.set(Calendar.MONTH, month - 1);
            calendar.set(Calendar.DAY_OF_MONTH, day);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            dt = new Date(calendar.getTimeInMillis());
        }
        return dt;
    }

    private Time binaryTime() {
        if (rawBytes.length == 0) {
            return null;
        }
        boolean negative = (rawBytes[0] == 0x01);
        int day = ((rawBytes[1] & 0xff)
                | ((rawBytes[2] & 0xff) << 8)
                | ((rawBytes[3] & 0xff) << 16)
                | ((rawBytes[4] & 0xff) << 24));
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        if (options.useLegacyDatetimeCode) {
            calendar.setLenient(false);
        }
        int hour = rawBytes[5];
        int minutes = rawBytes[6];
        int seconds = rawBytes[7];
        calendar.set(1970, 0, 1, hour, minutes, seconds);

        int nanoseconds = 0;
        if (rawBytes.length > 8) {
            nanoseconds = ((rawBytes[8] & 0xff)
                    | (rawBytes[9] & 0xff) << 8
                    | (rawBytes[10] & 0xff) << 16
                    | (rawBytes[11] & 0xff) << 24);
        }

        calendar.set(Calendar.MILLISECOND, nanoseconds / 1000);

        return new Time(calendar.getTimeInMillis());
    }


    private Timestamp binaryTimestamp(Calendar cal) throws ParseException {
        if (rawBytes.length == 0) {
            return null;
        }
        int year = 1970;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (dataType == MariaDbType.TIME) {
            return new Timestamp(getTime(cal).getTime());
        } else {
            year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
            month = rawBytes[2];
            day = rawBytes[3];
            if (rawBytes.length > 4) {
                hour = rawBytes[4];
                minutes = rawBytes[5];
                seconds = rawBytes[6];

                if (rawBytes.length > 7) {
                    microseconds = ((rawBytes[7] & 0xff)
                            | (rawBytes[8] & 0xff) << 8
                            | (rawBytes[9] & 0xff) << 16
                            | (rawBytes[10] & 0xff) << 24);
                }
            }
        }

        Calendar calendar = Calendar.getInstance();
        if (!options.useLegacyDatetimeCode) {
            calendar = cal;
        }
        Timestamp tt;
        synchronized (calendar) {
            calendar.set(year, month - 1, day, hour, minutes, seconds);
            tt = new Timestamp(calendar.getTimeInMillis());
        }
        tt.setNanos(microseconds * 1000);
        return tt;
    }

    private int extractNanos(String timestring) throws ParseException {
        int index = timestring.indexOf('.');
        if (index == -1) {
            return 0;
        }
        int nanos = 0;
        for (int i = index + 1; i < index + 10; i++) {
            int digit;
            if (i >= timestring.length()) {
                digit = 0;
            } else {
                char value = timestring.charAt(i);
                if (value < '0' || value > '9') {
                    throw new ParseException("cannot parse subsecond part in timestamp string '" + timestring + "'", i);
                }
                digit = value - '0';
            }
            nanos = nanos * 10 + digit;
        }
        return nanos;
    }

    /**
     * Get timeStamp from raw data.
     * @param cal session calendar.
     * @return timestamp.
     * @throws ParseException if text value cannot be parse
     */
    public Timestamp getTimestamp(Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
            String zeroTimestamp = "0000-00-00 00:00:00";
            if (rawValue.equals(zeroTimestamp)) {
                return null;
            }
            switch (dataType) {
                case TIME:
                    //time does not go after millisecond
                    Timestamp tt = new Timestamp(getTime(cal).getTime());
                    tt.setNanos(extractNanos(rawValue));
                    return tt;
                default:
                    try {
                        int year = Integer.parseInt(rawValue.substring(0, 4));
                        int month = Integer.parseInt(rawValue.substring(5, 7));
                        int day = Integer.parseInt(rawValue.substring(8, 10));
                        int hour = Integer.parseInt(rawValue.substring(11, 13));
                        int minutes = Integer.parseInt(rawValue.substring(14, 16));
                        int seconds = Integer.parseInt(rawValue.substring(17, 19));
                        int nanoseconds = extractNanos(rawValue);
                        Timestamp timestamp;
                        Calendar calendar = cal;
                        if (options.useLegacyDatetimeCode) {
                            calendar = Calendar.getInstance();
                        }
                        synchronized (calendar) {
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
                    } catch (NumberFormatException n) {
                        throw new ParseException("Value \"" + rawValue + "\" cannot be parse as Timestamp", 0);
                    } catch (StringIndexOutOfBoundsException s) {
                        throw new ParseException("Value \"" + rawValue + "\" cannot be parse as Timestamp", 0);
                    }
            }
        } else {
            return binaryTimestamp(cal);
        }

    }

    /**
     * Get inputStream value from raw data.
     * @return inputStream
     */
    public InputStream getInputStream() {
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(new String(rawBytes, StandardCharsets.UTF_8).getBytes());
    }

    /**
     * Get binaryInputStream value from raw data.
     * @return inputStream
     */
    public InputStream getBinaryInputStream() {
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(rawBytes);
    }

    /**
     * Get boolean value from raw data.
     * @return boolean
     */
    public boolean getBoolean() {
        if (rawBytes == null) {
            return false;
        }
        final String rawVal = new String(rawBytes, StandardCharsets.UTF_8);
        return rawVal.equalsIgnoreCase("true") || rawVal.equalsIgnoreCase("1") || (rawBytes[0] & 0x1) == 1;
    }

    /**
     * Is data null.
     * @return true if data is null
     */
    public boolean isNull() {
        String zeroTimestamp = "0000-00-00 00:00:00";
        String zeroDate = "0000-00-00";
        return (rawBytes == null
                || (isBinaryEncoded && ((dataType == MariaDbType.DATE || dataType == MariaDbType.TIMESTAMP || dataType == MariaDbType.DATETIME)
                && rawBytes.length == 0))
                || (!isBinaryEncoded && ((dataType == MariaDbType.TIMESTAMP || dataType == MariaDbType.DATETIME)
                && zeroTimestamp.equals(new String(rawBytes, StandardCharsets.UTF_8))))
                || (!isBinaryEncoded && (dataType == MariaDbType.DATE && zeroDate.equals(new String(rawBytes, StandardCharsets.UTF_8)))));
    }

    /**
     * Data length.
     * @return length
     */
    public int getDisplayLength() {
        if (rawBytes != null) {
            return rawBytes.length;
        }
        return 4; //NULL
    }

    /**
     * Get Blob from raw data
     * @return blob.
     */
    public Blob getBlob() {
        if (rawBytes == null) {
            return null;
        }
        return new MariaDbBlob(rawBytes);
    }

    /**
     * Get Clob from raw data
     * @return clob.
     */
    public Clob getClob() {
        if (rawBytes == null) {
            return null;
        }
        return new MariaDbClob(rawBytes);
    }

    /**
     * Get object value.
     * @param datatypeMappingFlags dataTypeflag (year is date or int, bit boolean or int,  ...)
     * @param cal session calendar
     * @return the object value.
     * @throws ParseException if data cannot be parse
     */
    public Object getObject(int datatypeMappingFlags, Calendar cal) throws ParseException {
        if (this.getBytes() == null) {
            return null;
        }
        switch (dataType) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return (getBytes()[0] != 0);
                }
                return getBytes();
            case TINYINT:
                if ((datatypeMappingFlags & TINYINT1_IS_BIT) != 0) {
                    if (columnInfo.getLength() == 1) {
                        return (getBytes()[0] != '0');
                    }
                }
                return getInt();
            case INTEGER:
                if (!columnInfo.isSigned()) {
                    return getLong();
                }
                return getInt();
            case BIGINT:
                if (!columnInfo.isSigned()) {
                    return getBigInteger();
                }
                return getLong();
            case DOUBLE:
                return getDouble();
            case TIMESTAMP:
                return getTimestamp(cal);
            case DATETIME:
                return getTimestamp(cal);
            case DATE:
                return getDate(cal);
            case VARCHAR:
                if (columnInfo.isBinary()) {
                    return getBytes();
                }
                return getString();
            case DECIMAL:
                return getBigDecimal();
            case BLOB:
                return getBytes();
            case LONGBLOB:
                return getBytes();
            case MEDIUMBLOB:
                return getBytes();
            case TINYBLOB:
                return getBytes();

            case NULL:
                return null;

            case YEAR:
                if ((datatypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
                    return getDate(cal);
                }
                return getShort();
            case SMALLINT:
            case MEDIUMINT:
                return getInt();
            case FLOAT:
                return getFloat();
            case TIME:
                return getTime(cal);
            case VARSTRING:
            case STRING:
                if (columnInfo.isBinary()) {
                    return getBytes();
                }
                return getString();
            case OLDDECIMAL:
                return getString();
            case GEOMETRY:
                return getBytes();
            case ENUM:
                break;
            case NEWDATE:
                break;
            case SET:
                break;
            default:
                break;
        }
        throw new RuntimeException(dataType.toString());
    }
}
