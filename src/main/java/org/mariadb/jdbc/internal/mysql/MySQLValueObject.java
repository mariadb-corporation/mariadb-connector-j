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
import org.mariadb.jdbc.internal.common.ValueObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Contains the raw value returned from the server
 *
 * Is immutable
 * 
 */
public class MySQLValueObject implements ValueObject {
    private MySQLColumnInformation columnInfo;
    private final byte[] rawBytes;
    private final MySQLType dataType;
    private final boolean isBinaryEncoded;

    public MySQLValueObject(byte[] rawBytes, MySQLColumnInformation columnInfo) {
        this.dataType = columnInfo.getType();
        this.rawBytes = rawBytes;
        this.isBinaryEncoded = false;
        this.columnInfo = columnInfo;
    }

    public MySQLValueObject(byte[] rawBytes, MySQLColumnInformation columnInfo, boolean isBinaryEncoded) {
        this.dataType = columnInfo.getType();
        this.rawBytes = rawBytes;
        this.isBinaryEncoded = isBinaryEncoded;
        this.columnInfo = columnInfo;
    }


    public String getString() {
        byte[] bytes = getBytes();
        if (bytes == null)
            return null;
        if (columnInfo.getType() == MySQLType.BIT && columnInfo.getLength() == 1)
            return (bytes[0] == 0)?"0":"1";

        if (rawBytes == null) {
            return null;
        }
        try {
            return new String(rawBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }
    }

    public byte getByte() {
        if (rawBytes == null) return 0;
        if (!this.isBinaryEncoded) {
            if (dataType == MySQLType.BIT) return rawBytes[0];
            try {
                return Byte.valueOf(getString());
            } catch (NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(getString());
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
                        return Byte.valueOf(getString());
                    } catch (NumberFormatException nfe) {
                        BigDecimal d = new BigDecimal(getString());
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
                return Short.valueOf(getString());
            } catch(NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(getString());
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
                        return Short.valueOf(getString());
                    } catch(NumberFormatException nfe) {
                        BigDecimal d = new BigDecimal(getString());
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
                return Integer.valueOf(getString());
            } catch(NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(getString());
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
                        return Integer.valueOf(getString());
                    } catch(NumberFormatException nfe) {
                        BigDecimal d = new BigDecimal(getString());
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
                return Long.valueOf(getString());
            } catch(NumberFormatException nfe) {
                BigDecimal d = new BigDecimal(getString());
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
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24
                            | (rawBytes[4] & 0xff) << 32
                            | (rawBytes[5] & 0xff) << 40
                            | (rawBytes[6] & 0xff) << 48
                            | (rawBytes[7] & 0xff) << 56
                    );
                    if (columnInfo.isSigned()) return x;
                    else {
                        return new BigInteger(1, new byte[] { (byte) (x >> 56),
                                (byte) (x >> 48), (byte) (x >> 40), (byte) (x >> 32),
                                (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8),
                                (byte) (x >> 0) }).longValue();
                    }
                case FLOAT:
                    return (long) getFloat();
                case DOUBLE:
                    return (long) getDouble();
                default:
                    try {
                        return Long.valueOf(getString());
                    } catch(NumberFormatException nfe) {
                        BigDecimal d = new BigDecimal(getString());
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
            return Float.valueOf(getString());
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
                    return Float.valueOf(getString());
            }
        }
    }


    public double getDouble() {
        if (rawBytes == null) return 0;
        if (!this.isBinaryEncoded) {
            return Double.valueOf(getString());
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
                            | (rawBytes[1] & 0xff) << 8
                            | (rawBytes[2] & 0xff) << 16
                            | (rawBytes[3] & 0xff) << 24
                            | (rawBytes[4] & 0xff) << 32
                            | (rawBytes[5] & 0xff) << 40
                            | (rawBytes[6] & 0xff) << 48
                            | (rawBytes[7] & 0xff) << 56);
                    return Double.longBitsToDouble(x);
                default:
                    return Double.valueOf(getString());
            }
        }
    }


    public BigDecimal getBigDecimal() {
        if (rawBytes == null) return BigDecimal.ONE;
        return new BigDecimal(getString());
    }

    public byte[] getBytes() {
        return rawBytes;
    }

    public BigInteger getBigInteger() {
        if (rawBytes == null) {
            return null;
        }
        return new BigInteger(getString());
    }

    public Date getDate(Calendar cal) throws ParseException {
        if (rawBytes == null) return null;

        String rawValue = getString();
        String zeroDate = "0000-00-00";
        if (rawValue.equals(zeroDate)) return null;

        if (!this.isBinaryEncoded) {
            SimpleDateFormat sdf;
            if (dataType == MySQLType.YEAR) {
                if (rawBytes.length == 2) {
                    sdf = new SimpleDateFormat("yy");
                } else {
                    sdf = new SimpleDateFormat("yyyy");
                }
            } else {
                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }
            if (cal != null) {
                sdf.setCalendar(cal);
            }
            java.util.Date utilDate = sdf.parse(rawValue);
            return new Date(utilDate.getTime());
        } else {
            switch (dataType) {
                case TIME:
                case DATE:
                case DATETIME:
                case TIMESTAMP:
                    return getByte();
        }
    }


    public Time getTime(Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        String rawValue = getString();
        String zeroDate = "0000-00-00";
        if (rawValue.equals(zeroDate)) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        //sdf.setLenient(false);
        if (cal != null) {
            sdf.setCalendar(cal);
        }
        final java.util.Date utilTime = sdf.parse(rawValue);
        long t0 = utilTime.getTime();
        int nanos = extractNanos(rawValue);
        int milliseconds = nanos/1000000;
        return new Time(t0+milliseconds);
    }

    private int extractNanos(String timestring) throws ParseException{
        int index = timestring.indexOf('.');
        if (index == -1)
            return 0;
        int nanos = 0;
        for(int i= index + 1; i < index + 10; i++) {
            int digit;
            if (i >= timestring.length()) {
                digit = 0;
            } else {
                char c = timestring.charAt(i);
                if (c < '0' || c > '9')
                    throw new ParseException("cannot parse subsecond part in timestamp string '" + timestring +"'", i);
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
        String rawValue = getString();
        String zeroTimestamp = "0000-00-00 00:00:00";
        if (rawValue.equals(zeroTimestamp)) {
            return null;
        }
        if (rawValue.length() >= 4  &&  rawValue.charAt(4) != '-') {
           /* This is probably a time value, since year separator is missing */
            Time t = getTime(cal);
            return new Timestamp(t.getTime());
        }

        SimpleDateFormat sdf;

        if (rawValue.length() > 11) {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        }
        sdf.setLenient(false);
        if (cal != null) {
            sdf.setCalendar(cal);
        }
        java.util.Date utilTime;
        try {
            utilTime = sdf.parse(rawValue);
        } catch (ParseException pe) {
            if (cal == null) {
                sdf.setCalendar(Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                utilTime = sdf.parse(rawValue);
            } else {
                throw pe;
            }
        }
        Timestamp ts = new Timestamp(utilTime.getTime());
        if(rawValue.indexOf('.') != -1) {
            ts.setNanos(extractNanos(rawValue));
        }
        return ts;
    }

    public InputStream getInputStream() {
        if (rawBytes == null) {
            return null;
        }
        return new ByteArrayInputStream(getString().getBytes());
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
        final String rawVal = getString();
        return rawVal.equalsIgnoreCase("true") || rawVal.equalsIgnoreCase("1") || (rawBytes[0] & 0x1)==1;
    }

    public boolean isNull() {
        String zeroTimestamp = "0000-00-00 00:00:00";
        String zeroDate = "0000-00-00";
        return (rawBytes == null
                || ((dataType == MySQLType.TIMESTAMP || dataType == MySQLType.DATETIME) && zeroTimestamp.equals(getString()))
                || (dataType== MySQLType.DATE && zeroDate.equals(getString()))
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
