package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Blob;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;

/**
 * Contains the raw value returned from the server
 * User: marcuse
 * Date: Feb 16, 2009
 * Time: 9:18:26 PM
 */
public class DrizzleValueObject implements ValueObject {
    private final byte[] rawBytes;
    private final DrizzleType dataType;

    public DrizzleValueObject(byte [] rawBytes, DrizzleType dataType) {
        this.rawBytes= rawBytes;
        this.dataType=dataType;
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

    public Time getTime() throws ParseException {
        if(rawBytes==null) return null;
        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        java.util.Date utilTime = sdf.parse(rawValue);
        return new Time(utilTime.getTime());
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
    public InputStream getInputStream(String s) throws UnsupportedEncodingException {
        if(rawBytes==null) return null;      
        return new ByteArrayInputStream(getString().getBytes("UTF-8"));

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
                return getBytes(); //TODO: wrong, handle this
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
         if(rawBytes==null) return null;
         String rawValue = getString();
         SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
         sdf.setCalendar(cal);
         java.util.Date utilTime = sdf.parse(rawValue);
         return new Time(utilTime.getTime());
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
