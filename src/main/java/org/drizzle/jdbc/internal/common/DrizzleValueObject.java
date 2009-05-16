/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.drizzle.DrizzleType;
import org.drizzle.jdbc.DrizzleBlob;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.util.Calendar;

/**
 * Contains the raw value returned from the server
 *
 * Is immutable
 *
 * User: marcuse
 * Date: Feb 16, 2009
 * Time: 9:18:26 PM
 */
public class DrizzleValueObject implements ValueObject {
    private final byte[] rawBytes;
    private final DrizzleType dataType;

    public DrizzleValueObject(byte [] rawBytes, DrizzleType dataType) {
        this.rawBytes = rawBytes;
        this.dataType = dataType;
    }

    public String getString() {
        if(rawBytes==null)
            return "NULL";
        return new String(rawBytes);
    }

    public long getLong() {
        if(rawBytes == null) return 0;
        return Long.valueOf(getString());
    }

    public int getInt() {
        if(rawBytes == null) return 0;
        return Integer.valueOf(getString());
    }

    public short getShort() {
        if(rawBytes == null) return 0;
        return Short.valueOf(getString());
    }

    public byte getByte() {
        if(rawBytes == null) return 0;
        return Byte.valueOf(getString());
    }

    public byte[] getBytes() {
        return rawBytes;
    }

    public float getFloat() {
        if(rawBytes == null) return 0;
        return Float.valueOf(getString());
    }

    public double getDouble() {
        if(rawBytes == null) return 0;
        return Double.valueOf(getString());
    }
    public BigDecimal getBigDecimal() {
        if(rawBytes == null) return null;
        return new BigDecimal(getString());
    }

    public Date getDate() throws ParseException {
        if(rawBytes==null) return null;
        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        java.util.Date utilDate = sdf.parse(rawValue);
        return new Date(utilDate.getTime());
    }

    /**
     * Since drizzle has no TIME datatype, JDBC Time is stored in a packed integer
     * 
     * @see Utils#packTime(long)
     * @see Utils#unpackTime(int) 
     * @return the time
     * @throws ParseException
     */
    public Time getTime() throws ParseException {
        if(rawBytes==null) return null;
        int packedValue = getInt();
        long timestamp = Utils.unpackTime(packedValue);
        return new Time(timestamp);
    }
    public Timestamp getTimestamp() throws ParseException {
        if(rawBytes==null) return null;
        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Date utilTime = sdf.parse(rawValue);
        return new Timestamp(utilTime.getTime());
    }    


    public InputStream getInputStream() {
        if(rawBytes==null) return null;
        return new ByteArrayInputStream(getString().getBytes());
    }

    public InputStream getBinaryInputStream() {
        if(rawBytes==null) return null;
        return new ByteArrayInputStream(rawBytes);
    }

    public Object getObject() throws ParseException {
        if(this.getBytes()==null)
            return null;
        switch(dataType) {
            case TINY:
                return getShort();
            case LONG:
                return getLong();
            case DOUBLE:
                return getDouble();
            case TIMESTAMP:
                return getTimestamp();
            case LONGLONG:
                return getLong();
            case TIME:
                return getTime();
            case DATETIME:
                return getTimestamp();
            case DATE:
                return getDate();
            case VARCHAR:
                return getString();
            case VIRTUAL:
                return getString();
            case NEWDECIMAL:
                return getBigDecimal();
            case ENUM:
                return getString();
            case BLOB:
                return new DrizzleBlob(getBytes());
        }                
        return null;
    }         

    public Date getDate(Calendar cal) throws ParseException {
        if(rawBytes==null) return null;
         String rawValue = getString();
         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
         sdf.setCalendar(cal);
         java.util.Date utilDate = sdf.parse(rawValue);
         return new Date(utilDate.getTime());
     }

     public Time getTime(Calendar cal) throws ParseException {
         // TODO: FIX! USE CAL!

         if(rawBytes==null) return null;
         int packedTime = getInt();
         long millis = Utils.unpackTime(packedTime);
         SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
         sdf.setCalendar(cal);
         return new Time(millis);
     }
     public Timestamp getTimestamp(Calendar cal) throws ParseException {
         if(rawBytes==null) return null;
         String rawValue = getString();
         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         sdf.setCalendar(cal);
         java.util.Date utilTime = sdf.parse(rawValue);
         return new Timestamp(utilTime.getTime());
     }

    public boolean getBoolean() {
        if(rawBytes==null) return false;

        String rawVal = getString();
        if(rawVal.toLowerCase().equals("true") || rawVal.toLowerCase().equals("1"))
            return true;
        return false;
    }

    public boolean isNull() {
        return rawBytes == null;
    }

    /**
     * for generated keys...
     * @param theLong the long to save int this VO
     * @return a new VO
     */
    public static ValueObject fromLong(long theLong) {
        return new DrizzleValueObject(String.valueOf(theLong).getBytes(), DrizzleType.LONG);
    }
    public int getDisplayLength() {
        if(rawBytes!=null)
            return rawBytes.length;
        return 4; //NULL
    }

}
