package org.mariadb.jdbc.internal.queryresults.resultset.value;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.text.ParseException;
import java.util.Calendar;


public interface ValueObject {

    public static final int TINYINT1_IS_BIT = 1;
    public static final int YEAR_IS_DATE_TYPE = 2;

    String getString(Calendar cal) throws SQLException;

    String getString() throws SQLException;

    long getLong() throws SQLException;

    int getInt() throws SQLException;

    short getShort() throws SQLException;

    byte getByte() throws SQLException;

    byte[] getBytes();

    float getFloat() throws SQLException;

    double getDouble() throws SQLException;

    BigDecimal getBigDecimal() throws SQLException;

    BigInteger getBigInteger() throws SQLException;

    InputStream getInputStream();

    InputStream getBinaryInputStream();

    Object getObject(int datatypeMappingFlags, Calendar cal) throws SQLException, ParseException;

    Date getDate(Calendar cal) throws ParseException;

    Time getTime(Calendar cal) throws ParseException;

    Timestamp getTimestamp(Calendar cal) throws ParseException;

    boolean getBoolean() throws SQLException;

    boolean isNull();

    Blob getBlob();
}
