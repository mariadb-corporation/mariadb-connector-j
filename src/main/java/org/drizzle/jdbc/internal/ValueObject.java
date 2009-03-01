package org.drizzle.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 16, 2009
 * Time: 9:16:36 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ValueObject {
    String getString();
    long getLong();
    int getInt();
    short getShort();
    byte getByte();
    byte [] getBytes();
    float getFloat();
    double getDouble();
    BigDecimal getBigDecimal();
    Date getDate() throws ParseException;
    Time getTime() throws ParseException;
    InputStream getInputStream();
    InputStream getInputStream(String s) throws UnsupportedEncodingException;
    Object getObject();
    Date getDate(Calendar cal) throws ParseException;
    Time getTime(Calendar cal) throws ParseException;
    Timestamp getTimestamp(Calendar cal) throws ParseException;
    Timestamp getTimestamp() throws ParseException;
    boolean getBoolean();

    boolean isNull();
}
