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

import org.mariadb.jdbc.MySQLBlob;
import org.mariadb.jdbc.MySQLClob;
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
 * Contains the raw value returned from the server
 *
 * Is immutable
 */
public class MySQLValueObject implements ValueObject {
    private final byte[] rawBytes;
    private final MySQLType dataType;
    private final boolean isBinaryEncoded;
    private final MySQLColumnInformation columnInfo;
    private final Options options;

    public MySQLValueObject(byte[] rawBytes, MySQLColumnInformation columnInfo, Options options) {
        this.dataType = columnInfo.getType();
        this.rawBytes = rawBytes;
        this.isBinaryEncoded = false;
        this.columnInfo = columnInfo;
        this.options = options;
    }

    public MySQLValueObject(byte[] rawBytes, MySQLColumnInformation columnInfo, boolean isBinaryEncoded, Options options) {
        this.dataType = columnInfo.getType();
        this.rawBytes = rawBytes;
        this.isBinaryEncoded = isBinaryEncoded;
        this.columnInfo = columnInfo;
        this.options = options;
    }


    public String getString() {
        return getString(null);
    }

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
                    } catch (ParseException e) {}
                }
                break;
            case TIMESTAMP:
            case DATETIME:
                if (isBinaryEncoded) {
                    try {
                        return getTimestamp(cal).toString();
                    } catch (ParseException e) {}
                }
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
            if (options.useLegacyDatetimeCode) return rawValue;
            if (rawValue.indexOf(".") > 0) return rawValue.substring(0, rawValue.indexOf("."));
            return rawValue;
        }

        if (rawBytes.length == 0) return null;

        boolean negative = (rawBytes[0] == 0x01);
        int day = ((rawBytes[1] & 0xff)
                | ((rawBytes[2] & 0xff) << 8)
                | ((rawBytes[3] & 0xff) << 16)
                | ((rawBytes[4] & 0xff) << 24));
        int hour = rawBytes[5];
        int minutes = rawBytes[6];
        int seconds = rawBytes[7];
        int microseconds = 0;
        if (rawBytes.length > 8) {
            microseconds = ((rawBytes[8] & 0xff)
                    | (rawBytes[9] & 0xff) << 8
                    | (rawBytes[10] & 0xff) << 16
                    | (rawBytes[11] & 0xff) << 24);
        }
        int timeHour = hour + day * 24;

        String hourString ;
        String minuteString;
        String secondString;
        String microsecondString = Integer.toString(microseconds);

        if (timeHour < 10) {
            hourString = "0" + timeHour;
        } else {
            hourString = Integer.toString(timeHour);
        }
        if (minutes < 10) {
            minuteString = "0" + minutes;
        } else {
            minuteString = Integer.toString(minutes);
        }
        if (seconds < 10) {
            secondString = "0" + seconds;
        } else {
            secondString = Integer.toString(seconds);
        }
        while (microsecondString.length() < 6) {
            microsecondString="0"+microsecondString;
        }
        return (negative?"-":"")+(hourString + ":" + minuteString + ":" + secondString);
    }

    public byte getByte() {
        if (rawBytes == null) return 0;
        if (!this.isBinaryEncoded) {
            if (dataType == MySQLType.BIT) return rawBytes[0];
            try {
                return Byte.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (d.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0)
                    return Byte.MIN_VALUE;
                if (d.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0)
                    return Byte.MAX_VALUE;
                return d.byteValue();
            }
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    if (columnInfo.isSigned()) return rawBytes[0];
                    else return (byte) (rawBytes[0] & 0xff);
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
                        BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (d.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) < 0)
                            return Byte.MIN_VALUE;
                        if (d.compareTo(BigDecimal.valueOf(Byte.MAX_VALUE)) > 0)
                            return Byte.MAX_VALUE;
                        return d.byteValue();
                    }
            }
        }
    }

    public short getShort() {
        if (rawBytes == null) return 0;
        if (!this.isBinaryEncoded) {
            try {
                return Short.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (d.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0)
                    return Short.MIN_VALUE;
                if (d.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0)
                    return Short.MAX_VALUE;
                return d.shortValue();
            }
        } else {
            switch (dataType) {
                case BIT:
                    return rawBytes[0];
                case TINYINT:
                    return getByte();
                case SMALLINT:
                case YEAR:
                    short x = (short) ((rawBytes[0] & 0xff) | ((rawBytes[1] & 0xff) << 8));
                    if (columnInfo.isSigned()) return x;
                    else return (short) (x & 0xffff);
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
                        BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (d.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0)
                            return Short.MIN_VALUE;
                        if (d.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0)
                            return Short.MAX_VALUE;
                        return d.shortValue();
                    }
            }
        }
    }


    public int getInt() {
        if (rawBytes == null) return 0;
        if (!this.isBinaryEncoded) {
            try {
                return Integer.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (d.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0)
                    return Integer.MIN_VALUE;
                if (d.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0)
                    return Integer.MAX_VALUE;
                return d.intValue();
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
                    int x = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    if (columnInfo.isSigned()) return x;
                    else return (x & 0xffffffff);
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
                        BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (d.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) < 0)
                            return Integer.MIN_VALUE;
                        if (d.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0)
                            return Integer.MAX_VALUE;
                        return d.intValue();
                    }
            }
        }
    }

    public long getLong() {
        if (rawBytes == null) return 0;
        if (!this.isBinaryEncoded) {
            try {
                return Long.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            } catch (NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                if (d.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0)
                    return Long.MIN_VALUE;
                if (d.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0)
                    return Long.MAX_VALUE;
                return d.longValue();
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
                    long x = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) return x;
                    else {
                        return new BigInteger(1, new byte[]{(byte) (x >> 56),
                                (byte) (x >> 48), (byte) (x >> 40), (byte) (x >> 32),
                                (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8),
                                (byte) (x >> 0)}).longValue();
                    }
                case FLOAT:
                    return (long) getFloat();
                case DOUBLE:
                    return (long) getDouble();
                default:
                    try {
                        return Long.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
                    } catch (NumberFormatException nfe) {
                        BigDecimal d = new BigDecimal(new String(rawBytes, StandardCharsets.UTF_8));
                        if (d.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0)
                            return Long.MIN_VALUE;
                        if (d.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0)
                            return Long.MAX_VALUE;
                        return d.longValue();
                    }
            }
        }
    }

    public float getFloat() {
        if (rawBytes == null) return 0;
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
                    int x = ((rawBytes[0] & 0xff)
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24);
                    return Float.intBitsToFloat(x);
                case DOUBLE:
                    return (float) getDouble();
                default:
                    return Float.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }
    }


    public double getDouble() {
        if (rawBytes == null) return 0;
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
                    long x = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56));
                    return Double.longBitsToDouble(x);
                default:
                    return Double.valueOf(new String(rawBytes, StandardCharsets.UTF_8));
            }
        }
    }


    public BigDecimal getBigDecimal() {
        if (rawBytes == null) return null;
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
                    long x = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned())
                        return new BigDecimal(String.valueOf(BigInteger.valueOf(x))).setScale(columnInfo.getDecimals());
                    else {
                        return new BigDecimal(String.valueOf(new BigInteger(1, new byte[]{(byte) (x >> 56),
                                (byte) (x >> 48), (byte) (x >> 40), (byte) (x >> 32),
                                (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8),
                                (byte) (x >> 0)}))).setScale(columnInfo.getDecimals());
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

    public BigInteger getBigInteger() {
        if (rawBytes == null) return null;
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
                    long x = ((rawBytes[0] & 0xff)
                            | ((long) (rawBytes[1] & 0xff) << 8)
                            | ((long) (rawBytes[2] & 0xff) << 16)
                            | ((long) (rawBytes[3] & 0xff) << 24)
                            | ((long) (rawBytes[4] & 0xff) << 32)
                            | ((long) (rawBytes[5] & 0xff) << 40)
                            | ((long) (rawBytes[6] & 0xff) << 48)
                            | ((long) (rawBytes[7] & 0xff) << 56)
                    );
                    if (columnInfo.isSigned()) return BigInteger.valueOf(x);
                    else {
                        return new BigInteger(1, new byte[]{(byte) (x >> 56),
                                (byte) (x >> 48), (byte) (x >> 40), (byte) (x >> 32),
                                (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8),
                                (byte) (x >> 0)});
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

    public Date getDate(Calendar cal) throws ParseException {
        if (rawBytes == null) return null;

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
                            Integer.parseInt(rawValue.substring(0, 4))  - 1900,
                            Integer.parseInt(rawValue.substring(5, 7))  - 1,
                            Integer.parseInt(rawValue.substring(8, 10))
                    );
                case YEAR:
                    int year = Integer.parseInt(rawValue);
                    if (rawBytes.length == 2) {
                        if (columnInfo.getLength() == 2)  {
                            if (year <= 69) year += 2000;
                            else year += 1900;
                        }
                    }

                    return new Date(year - 1900, 0, 1);
                default:
                    sdf = new SimpleDateFormat("yyyy-MM-dd");
                    if (cal != null) sdf.setCalendar(cal);
            }
            java.util.Date utilDate = sdf.parse(rawValue);
            return new Date(utilDate.getTime());
        } else {
            return binaryDate(cal);
        }
    }


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
                        if (raw.startsWith("-") || raw.split(":").length != 3 || raw.indexOf(":") > 3)
                            throw new ParseException("Time format \"" + raw + "\" incorrect, must be HH:mm:ss", 0);
                    }
                    boolean negate = raw.startsWith("-");
                    if (negate) raw = raw.substring(1);
                    String[] rawPart = raw.split(":");
                    if (rawPart.length == 3) {
                        int hour = Integer.parseInt(rawPart[0]);
                        int minutes = Integer.parseInt(rawPart[1]);
                        int seconds = Integer.parseInt(rawPart[2].substring(0, 2));
                        int nanoseconds = extractNanos(raw);
                        Calendar c = Calendar.getInstance();
                        if (options.useLegacyDatetimeCode) c.setLenient(true);
                        c.clear();
                        c.set(1970, 0, 1, (negate?-1:1)*hour, minutes, seconds);
                        c.set(Calendar.MILLISECOND, nanoseconds / 1000000);

                        return new Time(c.getTimeInMillis());
                    } else throw new ParseException(raw + " cannot be parse as time. time must have \"99:99:99\" format", 0);
            }
        } else {
            return binaryTime();
        }
    }

    private Date binaryDate(Calendar cal) throws ParseException {
        if (rawBytes.length == 0) return null;
        int year = 1970;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;

        year = ((rawBytes[0] & 0xff) | (rawBytes[1] & 0xff) << 8);
        month = rawBytes[2];
        day = rawBytes[3];

        Calendar c = Calendar.getInstance();
        /*if (!options.useLegacyDatetimeCode) {
            c = cal;
        }*/

        Date dt;
        synchronized (c) {
            c.clear();
            c.set(Calendar.YEAR, year);
            c.set(Calendar.MONTH, month - 1);
            c.set(Calendar.DAY_OF_MONTH, day);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MILLISECOND, 0);
            dt = new Date(c.getTimeInMillis());
        }
        return dt;
    }

    private Time binaryTime() {
        if (rawBytes.length == 0) return null;
        boolean negative = (rawBytes[0] == 0x01);
        int day = ((rawBytes[1] & 0xff)
                | ((rawBytes[2] & 0xff) << 8)
                | ((rawBytes[3] & 0xff) << 16)
                | ((rawBytes[4] & 0xff) << 24));
        int hour = rawBytes[5];
        int minutes = rawBytes[6];
        int seconds = rawBytes[7];
        int nanoseconds = 0;
        if (rawBytes.length > 8) {
            nanoseconds = ((rawBytes[8] & 0xff)
                    | (rawBytes[9] & 0xff) << 8
                    | (rawBytes[10] & 0xff) << 16
                    | (rawBytes[11] & 0xff) << 24);
        }

        Calendar c = Calendar.getInstance();
        c.clear();
        if (options.useLegacyDatetimeCode) c.setLenient(false);
        c.set(1970, 0, 1, hour, minutes, seconds);
        c.set(Calendar.MILLISECOND, nanoseconds / 1000);

        return new Time(c.getTimeInMillis());
    }



    private Timestamp binaryTimestamp(Calendar cal) throws ParseException {
        if (rawBytes.length == 0) return null;
        int year = 1970;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minutes = 0;
        int seconds = 0;
        int microseconds = 0;

        if (dataType == MySQLType.TIME) {
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

        Calendar c = Calendar.getInstance();
        if (!options.useLegacyDatetimeCode) {
            c = cal;
        }
        Timestamp tt;
        synchronized (c) {
            c.set(year, month - 1, day, hour, minutes, seconds);
            tt = new Timestamp(c.getTimeInMillis());
        }
        tt.setNanos(microseconds * 1000);
        return tt;
    }

    private int extractNanos(String timestring) throws ParseException {
        int index = timestring.indexOf('.');
        if (index == -1)
            return 0;
        int nanos = 0;
        for (int i = index + 1; i < index + 10; i++) {
            int digit;
            if (i >= timestring.length()) {
                digit = 0;
            } else {
                char c = timestring.charAt(i);
                if (c < '0' || c > '9')
                    throw new ParseException("cannot parse subsecond part in timestamp string '" + timestring + "'", i);
                digit = c - '0';
            }
            nanos = nanos * 10 + digit;
        }
        return nanos;
    }

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
                        Timestamp t;
                        Calendar c = cal;
                        if (options.useLegacyDatetimeCode) {
                            c = Calendar.getInstance();
                        }
                        synchronized (c) {
                            c.set(Calendar.YEAR, year);
                            c.set(Calendar.MONTH, month - 1);
                            c.set(Calendar.DAY_OF_MONTH, day);
                            c.set(Calendar.HOUR_OF_DAY, hour);
                            c.set(Calendar.MINUTE, minutes);
                            c.set(Calendar.SECOND, seconds);
                            c.set(Calendar.MILLISECOND, nanoseconds / 1000000);
                            t = new Timestamp(c.getTime().getTime());
                        }
                        t.setNanos(nanoseconds);
                        return t;
                    } catch (NumberFormatException n) {
                        throw new ParseException("Value \""+rawValue+"\" cannot be parse as Timestamp", 0);
                    } catch (StringIndexOutOfBoundsException s) {
                        throw new ParseException("Value \""+rawValue+"\" cannot be parse as Timestamp", 0);
                    }
            }
        } else {
            return binaryTimestamp(cal);
        }

    }

    public InputStream getInputStream() {
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(new String(rawBytes, StandardCharsets.UTF_8).getBytes());
    }

    public InputStream getBinaryInputStream() {
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(rawBytes);
    }

    public boolean getBoolean() {
        if (rawBytes == null) {
            return false;
        }
        final String rawVal = new String(rawBytes, StandardCharsets.UTF_8);
        return rawVal.equalsIgnoreCase("true") || rawVal.equalsIgnoreCase("1") || (rawBytes[0] & 0x1) == 1;
    }

    public boolean isNull() {
        String zeroTimestamp = "0000-00-00 00:00:00";
        String zeroDate = "0000-00-00";
        return (rawBytes == null
                || (isBinaryEncoded && ((dataType == MySQLType.DATE || dataType == MySQLType.TIMESTAMP || dataType == MySQLType.DATETIME) && rawBytes.length == 0))
                || (!isBinaryEncoded && ((dataType == MySQLType.TIMESTAMP || dataType == MySQLType.DATETIME) && zeroTimestamp.equals(new String(rawBytes, StandardCharsets.UTF_8))))
                || (!isBinaryEncoded && (dataType == MySQLType.DATE && zeroDate.equals(new String(rawBytes, StandardCharsets.UTF_8))))
        );
    }


    public int getDisplayLength() {
        if (rawBytes != null) {
            return rawBytes.length;
        }
        return 4; //NULL
    }

    public Blob getBlob() {
        if (rawBytes == null)
            return null;
        return new MySQLBlob(rawBytes);
    }

    public Clob getClob() {
        if (rawBytes == null)
            return null;
        return new MySQLClob(rawBytes);
    }


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
                if (columnInfo.isBinary())
                    return getBytes();
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
                if (columnInfo.isBinary()) return getBytes();
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
