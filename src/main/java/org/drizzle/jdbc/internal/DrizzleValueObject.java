package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.packet.buffer.ReadUtil;
import org.drizzle.jdbc.internal.packet.buffer.WriteBuffer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

/**
 * Contains the raw value returned from the server
 * User: marcuse
 * Date: Feb 16, 2009
 * Time: 9:18:26 PM
 */
public class DrizzleValueObject implements ValueObject {
    public enum DataType {
        INTEGER, STRING, LONG
    }
    private final byte[] rawBytes;
    private final DataType dataType;
    public DrizzleValueObject(byte [] rawBytes, DataType dataType) {
        this.rawBytes= rawBytes;
        this.dataType=dataType;
    }

    public String getString() {
        if(rawBytes==null)
            return "NULL";
        return new String(rawBytes);
    }

    public long getLong() {
        return Long.valueOf(getString());
    }

    public int getInt() {
        return Integer.valueOf(getString());
    }

    public short getShort() {
        return Short.valueOf(getString());
    }

    public byte getByte() {
        return Byte.valueOf(getString());
    }

    public byte[] getBytes() {
        return rawBytes;
    }

    public float getFloat() {
        return Float.valueOf(getString());
    }

    public double getDouble() {
        return Double.valueOf(getString());
    }
    public BigDecimal getBigDecimal() {
        return new BigDecimal(getString());
    }

    public Date getDate() throws ParseException {
        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return (Date) sdf.parse(rawValue);
    }

    public Time getTime() throws ParseException {
        //todo: is this proper?

        String rawValue = getString();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        return (Time)sdf.parse(rawValue);
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(getString().getBytes());
    }
    public InputStream getInputStream(String s) throws UnsupportedEncodingException {
        return new ByteArrayInputStream(getString().getBytes("UTF-8"));

    }

    public Object getObject() {
        if(this.getBytes()==null)
            return null;
        switch(this.dataType) {
            case LONG:
                return getLong();
            case STRING:
                return getString();
            case INTEGER:
                return getInt();
                
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * for generated keys...
     * @param theLong the long to save int this VO
     * @return a new VO
     */
    public static ValueObject fromLong(long theLong) {
        return new DrizzleValueObject(String.valueOf(theLong).getBytes(), DataType.LONG);
    }
}
