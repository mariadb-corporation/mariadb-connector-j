/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Calendar;

/**
 * .
 * User: marcuse
 * Date: Feb 16, 2009
 * Time: 9:16:36 PM
 */
public interface ValueObject {
    String getString();

    long getLong();

    int getInt();

    short getShort();

    byte getByte();

    byte[] getBytes();

    float getFloat();

    double getDouble();

    BigDecimal getBigDecimal();

    Date getDate() throws ParseException;

    Time getTime();

    InputStream getInputStream();

    InputStream getBinaryInputStream();

    Object getObject() throws ParseException;

    Date getDate(Calendar cal) throws ParseException;

    Time getTime(Calendar cal);

    Timestamp getTimestamp(Calendar cal) throws ParseException;

    Timestamp getTimestamp() throws ParseException;

    boolean getBoolean();

    boolean isNull();

    int getDisplayLength();
}
