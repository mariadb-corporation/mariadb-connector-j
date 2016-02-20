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

package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * Contains the raw value returned from the server.
 * Is immutable
 */
@SuppressWarnings("deprecation")
public class MariaDbValueObject implements ValueObject {

    private static final Pattern isIntegerRegex = Pattern.compile("^-?\\d+\\.0+$");
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


    public String getString() throws SQLException {
        return getString(null);
    }

    /**
     * Get String from raw data.
     * @param cal session calendar
     * @return string
     */
    public String getString(Calendar cal) throws SQLException {
        if (rawBytes == null) {
            return null;
        }

        switch (columnInfo.getType()) {
            case BIT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    return (rawBytes[0] == 0) ? "0" : "1";
                }
                break;
            case TINYINT:
                if (this.isBinaryEncoded) {
                    return String.valueOf(getTinyInt());
                }
                break;
            case SMALLINT:
                if (this.isBinaryEncoded) {
                    return String.valueOf(getSmallInt());
                }
                break;
            case INTEGER:
            case MEDIUMINT:
                if (this.isBinaryEncoded) {
                    return String.valueOf(getMediumInt());
                }
                break;
            case BIGINT:
                if (this.isBinaryEncoded) {
                    if (!columnInfo.isSigned()) {
                        return String.valueOf(getBigInteger());
                    }
                    return String.valueOf(getLong());
                }
                break;
            case DOUBLE:
                return String.valueOf(getDouble());
            case FLOAT:
                return String.valueOf(getFloat());
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
            case YEAR:
                if (options.yearIsDateType) {
                    try {
                        return getDate(cal).toString();
                    } catch (ParseException e) {
                        //eat exception
                    }
                }
                if (this.isBinaryEncoded) {
                    return String.valueOf(getSmallInt());
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                try {
                    return getTimestamp(cal).toString();
                } catch (ParseException e) {
                }
                break;
            case DECIMAL:
                return getBigDecimal().toString();
            case GEOMETRY:
                return new String(getBytes());
            case NULL:
                return null;
            case OLDDECIMAL:
                return getBigDecimal().toString();
            default:
                return new String(rawBytes, StandardCharsets.UTF_8);
        }
        return new String(rawBytes, StandardCharsets.UTF_8);
    }

    private String getTimeString() {
        if (rawBytes == null || rawBytes.length == 0) {
            return null;
        }
        String rawValue = new String(rawBytes, StandardCharsets.UTF_8);
        if ( "0000-00-00".equals(rawValue)) {
            return null;
        }
        if (!this.isBinaryEncoded) {
            if (!options.useLegacyDatetimeCode && rawValue.indexOf(".") > 0) {
                return rawValue.substring(0, rawValue.indexOf("."));
            }
            return rawValue;
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
        return (negative ? "-" : "") + (hourString + ":" + minuteString + ":" + secondString + "." + microsecondString);
    }



    /**
     * Get byte from raw data.
     * @return byte
     */
    public byte getByte() throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            if (dataType == MariaDbType.BIT) {
                return rawBytes[0];
            }
            return parseByte();
        } else {
            long value;
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt();
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt();
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt();
                    break;
                case BIGINT:
                    value = getLong();
                    break;
                case FLOAT:
                    value = (long) getFloat();
                    break;
                case DOUBLE:
                    value = (long) getDouble();
                    break;
                default:
                    return parseByte();
            }
            rangeCheck(Byte.class, Byte.MIN_VALUE, Byte.MAX_VALUE, value);
            return (byte) value;
        }
    }

    private void rangeCheck(Object className, long minValue, long maxValue, long value) throws SQLException {
        if (value < minValue || value > maxValue) {
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value + " is not in "
                    + className + " range", "22003", 1264);
        }
    }

    private int getTinyInt() throws SQLException {
        int value = rawBytes[0];
        if (!columnInfo.isSigned()) {
            value = (rawBytes[0] & 0xff);
        }
        return value;
    }

    private int getSmallInt() throws SQLException {
        int value = ((rawBytes[0] & 0xff) | ((rawBytes[1] & 0xff) << 8));
        if (!columnInfo.isSigned()) {
            return value & 0xffff;
        }
        //short cast here is important : -1 will be received as -1, -1 -> 65535
        return (short) value;
    }

    private long getMediumInt() throws SQLException {
        long value = ((rawBytes[0] & 0xff)
                | (rawBytes[1] & 0xff) << 8
                | (rawBytes[2] & 0xff) << 16
                | (rawBytes[3] & 0xff) << 24);
        if (!columnInfo.isSigned()) {
            value = value & 0xffffffffL ;
        }
        return value;
    }


    private byte parseByte() throws SQLException {
        String value = new String(rawBytes, StandardCharsets.UTF_8);
        try {
            switch (dataType) {
                case FLOAT:
                    Float floatValue = Float.valueOf(value);
                    if (floatValue.compareTo((float) Byte.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Byte range", "22003", 1264);
                    }
                    return floatValue.byteValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(value);
                    if (doubleValue.compareTo((double) Byte.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Byte range", "22003", 1264);
                    }
                    return doubleValue.byteValue();
                default:
                    return Byte.parseByte(value);
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getByte with a database decimal value
            //retrying without the decimal part.
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Byte.parseByte(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException nfee) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Byte range",
                    "22003", 1264);
        }
    }

    /**
     * Get short from raw data.
     * @return short
     */
    public short getShort() throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return parseShort();
        } else {
            long value;
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt();
                    break;
                case SMALLINT:
                case YEAR:
                    value = ((rawBytes[0] & 0xff) | ((rawBytes[1] & 0xff) << 8));
                    if (columnInfo.isSigned()) {
                        return (short) value;
                    }
                    value = value & 0xffff;
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt();
                    break;
                case BIGINT:
                    value = getLong();
                    break;
                case FLOAT:
                    value = (long) getFloat();
                    break;
                case DOUBLE:
                    value = (long) getDouble();
                    break;
                default:
                    return parseShort();
            }
            rangeCheck(Short.class, Short.MIN_VALUE, Short.MAX_VALUE, value);
            return (short) value;
        }
    }

    private short parseShort() throws SQLException {
        String value = new String(rawBytes, StandardCharsets.UTF_8);
        try {
            switch (dataType) {
                case FLOAT:
                    Float floatValue = Float.valueOf(value);
                    if (floatValue.compareTo((float) Short.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Short range", "22003", 1264);
                    }
                    return floatValue.shortValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(value);
                    if (doubleValue.compareTo((double) Short.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Short range", "22003", 1264);
                    }
                    return doubleValue.shortValue();
                default:
                    return Short.parseShort(value);
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getInt with a database decimal value
            //retrying without the decimal part.
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Short.parseShort(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException numberFormatException) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Short range", "22003", 1264);
        }
    }

    /**
     * Get int from raw data.
     * @return int
     */
    public int getInt() throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return parseInt();
        } else {
            long value;
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt();
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt();
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    if (columnInfo.isSigned()) {
                        return (int) value;
                    } else if (value < 0) {
                        value = value & 0xffffffffL;
                    }
                    break;
                case BIGINT:
                    value = getLong();
                    break;
                case FLOAT:
                    value = (long) getFloat();
                    break;
                case DOUBLE:
                    value = (long) getDouble();
                    break;
                default:
                    return parseInt();
            }
            rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value);
            return (int) value;
        }
    }

    private int parseInt() throws SQLException {
        String value = new String(rawBytes, StandardCharsets.UTF_8);
        try {
            switch (dataType) {
                case FLOAT:
                    Float floatValue = Float.valueOf(value);
                    if (floatValue.compareTo((float) Integer.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Integer range", "22003", 1264);
                    }
                    return floatValue.intValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(value);
                    if (doubleValue.compareTo((double) Integer.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Integer range", "22003", 1264);
                    }
                    return doubleValue.intValue();
                default:
                    return Integer.parseInt(value);
            }
        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getInt with a database decimal value
            //retrying without the decimal part.
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Integer.parseInt(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException numberFormatException) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Integer range", "22003", 1264);
        }
    }

    /**
     * Get long from raw data.
     * @return long
     */
    public long getLong() throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return parseLong();
        } else {
            long value;
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt();
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt();
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt();
                    break;
                case BIGINT:
                    value = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56));
                    if (columnInfo.isSigned()) {
                        return value;
                    }
                    BigInteger unsignedValue = new BigInteger(1, new byte[]{(byte) (value >> 56),
                            (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                            (byte) (value >> 0)});
                    if (unsignedValue.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value "
                                + unsignedValue + " is not in Long range", "22003", 1264);
                    }
                    return unsignedValue.longValue();
                case FLOAT:
                    Float floatValue = getFloat();
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + floatValue
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = getDouble();
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + doubleValue
                                + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                default:
                    return parseLong();
            }
            rangeCheck(Long.class, Long.MIN_VALUE, Long.MAX_VALUE, value);
            return value;

        }
    }

    private long parseLong() throws SQLException {
        String value = new String(rawBytes, StandardCharsets.UTF_8);
        try {
            switch (dataType) {
                case FLOAT:
                    Float floatValue = Float.valueOf(value);
                    if (floatValue.compareTo((float) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Long range", "22003", 1264);
                    }
                    return floatValue.longValue();
                case DOUBLE:
                    Double doubleValue = Double.valueOf(value);
                    if (doubleValue.compareTo((double) Long.MAX_VALUE) >= 1) {
                        throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                                + " is not in Long range", "22003", 1264);
                    }
                    return doubleValue.longValue();
                default:
                    return Long.parseLong(value);
            }

        } catch (NumberFormatException nfe) {
            //parse error.
            //if this is a decimal with only "0" in decimal, like "1.0000" (can be the case if trying to getlong with a database decimal value
            //retrying without the decimal part.
            if (isIntegerRegex.matcher(value).find()) {
                try {
                    return Long.parseLong(value.substring(0, value.indexOf(".")));
                } catch (NumberFormatException nfee) {
                    //eat exception
                }
            }
            throw new SQLException("Out of range value for column '" + columnInfo.getName() + "' : value " + value
                    + " is not in Long range", "22003", 1264);
        }
    }

    /**
     * Get float from raw data.
     * @return float
     */
    public float getFloat() throws SQLException {
        if (rawBytes == null) {
            return 0;
        }
        if (!this.isBinaryEncoded) {
            return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
        } else {
            long value;
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    value = getTinyInt();
                    break;
                case SMALLINT:
                case YEAR:
                    value = getSmallInt();
                    break;
                case INTEGER:
                case MEDIUMINT:
                    value = getMediumInt();
                    break;
                case BIGINT:
                    value = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56));
                    if (columnInfo.isSigned()) {
                        return value;
                    }
                    BigInteger unsignedValue = new BigInteger(1, new byte[]{(byte) (value >> 56),
                            (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
                            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                            (byte) (value >> 0)});
                    return unsignedValue.floatValue();
                case FLOAT:
                    int valueFloat = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    return Float.intBitsToFloat(valueFloat);
                case DOUBLE:
                    return (float) getDouble();
                default:
                    return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
            return Float.valueOf(String.valueOf(value));
        }
    }

    /**
     * Get double value from raw data.
     * @return double
     */
    public double getDouble() throws SQLException {
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
                    return getTinyInt();
                case SMALLINT:
                case YEAR:
                    return getSmallInt();
                case INTEGER:
                case MEDIUMINT:
                    return getMediumInt();
                case BIGINT:
                    long valueLong = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) {
                        return valueLong;
                    } else {
                        return new BigInteger(1, new byte[]{(byte) (valueLong >> 56),
                                (byte) (valueLong >> 48), (byte) (valueLong >> 40), (byte) (valueLong >> 32),
                                (byte) (valueLong >> 24), (byte) (valueLong >> 16), (byte) (valueLong >> 8),
                                (byte) (valueLong >> 0)}).doubleValue();
                    }
                case FLOAT:
                    return getFloat();
                case DOUBLE:
                    long valueDouble = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56));
                    return Double.longBitsToDouble(valueDouble);
                default:
                    return Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Get BigDecimal from rax data.
     * @return Bigdecimal value
     */
    public BigDecimal getBigDecimal() throws SQLException {
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
                    return BigDecimal.valueOf((long) getTinyInt());
                case SMALLINT:
                case YEAR:
                    return BigDecimal.valueOf((long) getSmallInt());
                case INTEGER:
                case MEDIUMINT:
                    return BigDecimal.valueOf(getMediumInt());
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
                    return BigDecimal.valueOf(getFloat());
                case DOUBLE:
                    return BigDecimal.valueOf(getDouble());
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
    public BigInteger getBigInteger() throws SQLException {
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
                    return BigInteger.valueOf((long) (columnInfo.isSigned() ? getByte() : (rawBytes[0] & 0xff)));
                case SMALLINT:
                case YEAR:
                    short valueShort = (short) ((rawBytes[0] & 0xff) | ((rawBytes[1] & 0xff) << 8));
                    return BigInteger.valueOf((long) (columnInfo.isSigned() ? valueShort : (valueShort & 0xffff)));
                case INTEGER:
                case MEDIUMINT:
                    int valueInt = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    return BigInteger.valueOf(((columnInfo.isSigned()) ? valueInt : (valueInt >= 0) ? valueInt : valueInt & 0xffffffffL));
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
                    if (rawBytes.length == 2 && columnInfo.getLength() == 2) {
                        if (year <= 69) {
                            year += 2000;
                        } else {
                            year += 1900;
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
        String raw = new String(rawBytes, StandardCharsets.UTF_8);
        String zeroDate = "0000-00-00";
        if (raw.equals(zeroDate)) {
            return null;
        }

        if (!this.isBinaryEncoded) {
            if (dataType == MariaDbType.TIMESTAMP || dataType == MariaDbType.DATETIME) {
                return new Time(getTimestamp(cal).getTime());
            } else if (dataType == MariaDbType.DATE) {
                Calendar zeroCal = Calendar.getInstance();
                zeroCal.set(1970, 0, 1, 0, 0, 0);
                zeroCal.set(Calendar.MILLISECOND, 0);
                return new Time(zeroCal.getTimeInMillis());
            } else {
                if (!options.useLegacyDatetimeCode && (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3)) {
                    throw new ParseException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss", 0);
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
            return binaryTime(cal);
        }
    }

    private Date binaryDate(Calendar cal) throws ParseException {
        switch (dataType) {
            case TIMESTAMP:
            case DATETIME:
                return new Date(getTimestamp(cal).getTime());
            default:
                if (rawBytes.length == 0) {
                    return null;
                }
                int year;
                int month;
                int day;

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
    }

    private Time binaryTime(Calendar cal) throws ParseException {
        switch (dataType) {
            case TIMESTAMP:
            case DATETIME:
                Timestamp ts = binaryTimestamp(cal);
                return new Time(ts.getTime());
            case DATE:
                Calendar tmpCalendar = Calendar.getInstance();
                tmpCalendar.clear();
                tmpCalendar.set(1970, 0, 1, 0, 0, 0);
                tmpCalendar.set(Calendar.MILLISECOND, 0);
                return new Time(tmpCalendar.getTimeInMillis());
            default:
                Calendar calendar = Calendar.getInstance();
                calendar.clear();
                int day = 0;
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                boolean negate = false;
                if (rawBytes.length > 0) {
                    negate = (rawBytes[0] & 0xff) == 0x01;
                }
                if (rawBytes.length > 4) {
                    day = ((rawBytes[1] & 0xff)
                            | (rawBytes[2] & 0xff) << 8
                            | (rawBytes[3] & 0xff) << 16
                            | (rawBytes[4] & 0xff) << 24);
                }
                if (rawBytes.length > 7) {
                    hour = rawBytes[5];
                    minutes = rawBytes[6];
                    seconds = rawBytes[7];
                }
                calendar.set(1970, 0, ((negate ? -1 : 1) * day) + 1, (negate ? -1 : 1) * hour, minutes, seconds);

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
    }


    private Timestamp binaryTimestamp(Calendar cal) throws ParseException {
        if (rawBytes.length == 0) {
            return null;
        }
        int year;
        int month;
        int day = 0;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (dataType == MariaDbType.TIME) {
            Calendar calendar = Calendar.getInstance();
            calendar.clear();

            boolean negate = false;
            if (rawBytes.length > 0) {
                negate = (rawBytes[0] & 0xff) == 0x01;
            }
            if (rawBytes.length > 4) {
                day = ((rawBytes[1] & 0xff)
                        | (rawBytes[2] & 0xff) << 8
                        | (rawBytes[3] & 0xff) << 16
                        | (rawBytes[4] & 0xff) << 24);
            }
            if (rawBytes.length > 7) {
                hour = rawBytes[5];
                minutes = rawBytes[6];
                seconds = rawBytes[7];
            }

            if (rawBytes.length > 8) {
                microseconds = ((rawBytes[8] & 0xff)
                        | (rawBytes[9] & 0xff) << 8
                        | (rawBytes[10] & 0xff) << 16
                        | (rawBytes[11] & 0xff) << 24);
            }

            calendar.set(1970, 0, ((negate ? -1 : 1) * day) + 1, (negate ? -1 : 1) * hour, minutes, seconds);
            Timestamp tt = new Timestamp(calendar.getTimeInMillis());
            tt.setNanos(microseconds * 1000);
            return tt;
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
    public boolean getBoolean() throws SQLException {
        if (rawBytes == null) {
            return false;
        }
        if (!this.isBinaryEncoded) {
            if (rawBytes.length == 1 && rawBytes[0] == 0) {
                return false;
            }
            final String rawVal = new String(rawBytes, StandardCharsets.UTF_8);
            return !("false".equals(rawVal) || "0".equals(rawVal));
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0] != 0;
                case TINYINT:
                    return getTinyInt() != 0;
                case SMALLINT:
                case YEAR:
                    return getSmallInt() != 0;
                case INTEGER:
                case MEDIUMINT:
                    return getMediumInt() != 0;
                case BIGINT:
                    return getLong() != 0;
                case FLOAT:
                    return getFloat() != 0;
                case DOUBLE:
                    return getDouble() != 0;
                default:
                    final String rawVal = new String(rawBytes, StandardCharsets.UTF_8);
                    return !("false".equals(rawVal) || "0".equals(rawVal));
            }
        }
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
     * @param dataTypeMappingFlags dataTypeflag (year is date or int, bit boolean or int,  ...)
     * @param cal session calendar
     * @return the object value.
     * @throws ParseException if data cannot be parse
     */
    public Object getObject(int dataTypeMappingFlags, Calendar cal) throws SQLException, ParseException {
        if (rawBytes == null) {
            return null;
        }

        switch (dataType) {
            case BIT:
                if (columnInfo.getLength() == 1) {
                    return rawBytes[0] != 0;
                }
                return rawBytes;
            case TINYINT:
                if (options.tinyInt1isBit && columnInfo.getLength() == 1) {
                    if (!this.isBinaryEncoded) {
                        return rawBytes[0] != '0';
                    } else {
                        return rawBytes[0] != 0;
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
            case LONGBLOB:
            case MEDIUMBLOB:
            case TINYBLOB:
                return getBytes();
            case NULL:
                return null;
            case YEAR:
                if ((dataTypeMappingFlags & YEAR_IS_DATE_TYPE) != 0) {
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
