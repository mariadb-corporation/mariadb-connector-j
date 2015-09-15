package org.mariadb.jdbc.internal.common;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.util.Calendar;


public interface ValueObject {

    public static final int TINYINT1_IS_BIT = 1;
    public static final int YEAR_IS_DATE_TYPE = 2;

    String getString();

    long getLong();

    int getInt();

    short getShort();

    byte getByte();

    byte[] getBytes();

    float getFloat();

    double getDouble();

    BigDecimal getBigDecimal();

    BigInteger getBigInteger();

    InputStream getInputStream();

    InputStream getBinaryInputStream();

    Object getObject(int datatypeMappingFlags, Calendar cal) throws ParseException;

    Date getDate(Calendar cal) throws ParseException;

    Time getTime(Calendar cal) throws ParseException;

    Timestamp getTimestamp(Calendar cal) throws ParseException;

    boolean getBoolean();

    boolean isNull();

    int getDisplayLength();

    Clob getClob();

    Blob getBlob();
}
