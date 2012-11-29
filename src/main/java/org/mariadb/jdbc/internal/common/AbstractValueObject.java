/*
MariaDB Client for Java

Copyright (c) 2012 Monty Program Ab. All Rights Reserved.

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

All rights reserved.

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

package org.mariadb.jdbc.internal.common;

import org.mariadb.jdbc.MySQLBlob;
import org.mariadb.jdbc.MySQLClob;
import org.mariadb.jdbc.internal.mysql.MySQLType;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Jun 10, 2009 Time: 4:13:03 PM To change this template use File |
 * Settings | File Templates.
 */
public abstract class AbstractValueObject implements ValueObject {
    private final byte[] rawBytes;
    protected final DataType dataType;

    protected AbstractValueObject(final byte[] rawBytes, final DataType dataType) {
        this.dataType = dataType;
        this.rawBytes = rawBytes;
    }

    public String getString() {
        if (rawBytes == null) {
            return null;
        }
        try {
           return new String(rawBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
           throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
       }
    }

    public long getLong() {
        if (rawBytes == null) {
            return 0;
        }
        try {
            return Long.valueOf(getString());
        } catch(NumberFormatException nfe) {
            return (long) Double.parseDouble(getString());
        }
    }

    public int getInt() {
        if (rawBytes == null) {
            return 0;
        }
        try {
            return Integer.valueOf(getString());
        } catch(NumberFormatException nfe) {
            return (int) Double.parseDouble(getString());
        }
    }

    public short getShort() {
        if (rawBytes == null) {
            return 0;
        }
        try {
            return Short.valueOf(getString());
        } catch(NumberFormatException nfe) {
           return (short) Double.parseDouble(getString());
        }
    }

    public byte getByte() {
        if (rawBytes == null) {
            return 0;
        }
        switch(dataType.getType()) {
            case BIT:
                return rawBytes[0];
        }
        return Byte.valueOf(getString());
    }

    public byte[] getBytes() {
        return rawBytes;
    }

    public float getFloat() {
        if (rawBytes == null) {
            return 0;
        }
        return Float.valueOf(getString());
    }

    public double getDouble() {
        if (rawBytes == null) {
            return 0;
        }
        return Double.valueOf(getString());
    }

    public BigDecimal getBigDecimal() {
        if (rawBytes == null) {
            return null;
        }
        return new BigDecimal(getString());
    }
    public BigInteger getBigInteger() {
        if (rawBytes == null) {
            return null;
        }
        return new BigInteger(getString());
    }
    public Date getDate() throws ParseException {
        if (rawBytes == null) {
            return null;
        }

        String rawValue = getString();
        SimpleDateFormat sdf;
        if (dataType.getType() == MySQLType.Type.YEAR) {
           if (rawBytes.length == 2) {
              sdf = new SimpleDateFormat("yy");
           } else {
              sdf = new SimpleDateFormat("yyyy");
           }
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        }
        java.util.Date utilDate = sdf.parse(rawValue);
        return new Date(utilDate.getTime());
    }

    /**
     * Since drizzle has no TIME datatype, JDBC Time is stored in a packed integer
     *
     * @return the time
     * @throws java.text.ParseException
     * @see Utils#packTime(long)
     * @see Utils#unpackTime(int)
     */
    public Time getTime() throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        //sdf.setLenient(false);
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

    public Timestamp getTimestamp() throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        String rawValue = getString();
        SimpleDateFormat sdf;

        if (rawValue.length() > 11) {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        } else {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        }
        sdf.setLenient(false);
        final java.util.Date utilTime = sdf.parse(rawValue);
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
    
    public abstract Object getObject(int datatypeMappingFlags) throws ParseException;

    public Date getDate(final Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        final String rawValue = getString();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setCalendar(cal);
        final java.util.Date utilDate = sdf.parse(rawValue);
        return new Date(utilDate.getTime());
    }

    public Time getTime(final Calendar cal) throws ParseException{
        if (rawBytes == null) {
            return null;
        }
        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        sdf.setCalendar(cal);
        return new Time(sdf.parse(rawValue).getTime());
    }

    public Timestamp getTimestamp(final Calendar cal) throws ParseException {
        if (rawBytes == null) {
            return null;
        }
        final String rawValue = getString();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setCalendar(cal);
        final java.util.Date utilTime = sdf.parse(rawValue);
        return new Timestamp(utilTime.getTime());
    }

    public boolean getBoolean() {
        if (rawBytes == null) {
            return false;
        }
        final String rawVal = getString();
        return rawVal.equalsIgnoreCase("true") || rawVal.equalsIgnoreCase("1") || (rawBytes[0] & 0x1)==1;
    }

    public boolean isNull() {
        return rawBytes == null;
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
}
