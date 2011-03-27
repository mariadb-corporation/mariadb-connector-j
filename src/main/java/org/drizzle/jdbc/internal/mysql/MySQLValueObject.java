/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql;

import org.drizzle.jdbc.DrizzleBlob;
import org.drizzle.jdbc.internal.common.AbstractValueObject;
import org.drizzle.jdbc.internal.common.DataType;

import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Contains the raw value returned from the server
 * <p/>
 * Is immutable
 * <p/>
 * User: marcuse Date: Feb 16, 2009 Time: 9:18:26 PM
 */
public class MySQLValueObject extends AbstractValueObject {
    public MySQLValueObject(final byte[] rawBytes, final DataType dataType) {
        super(rawBytes, dataType);
    }

    public Object getObject() throws ParseException {
        if (this.getBytes() == null) {
            return null;
        }
        switch (dataType.getType()) {
            case TINY:
                return getShort();
            case LONG:
                return getLong();
            case DOUBLE:
                return getDouble();
            case TIMESTAMP:
                return getTimestamp();
            case LONGLONG:
                return getBigInteger();
            case DATETIME:
                return getTimestamp();
            case DATE:
                return getDate();
            case VARCHAR:
                return getString();
            case NEWDECIMAL:
                return getBigDecimal();
            case ENUM:
                return getString();
            case BLOB:
                return new DrizzleBlob(getBytes());
            case YEAR:
                return getString();
            case BIT:
                if(getBytes().length == 1) {
                    return getBytes()[0] == 1;
                }
                return null;
            case SHORT:
            case INT24:
                return getInt();
            case FLOAT:
                return getFloat();
            case TIME:
                return getTime();
            case CLOB:
                return getString();
        }
        return null;
    }

    @Override
    public Time getTime() throws ParseException {
        if (getBytes() == null) {
            return null;
        }
        final String rawValue = getString();
        final SimpleDateFormat sdf;
        sdf = new SimpleDateFormat("HH:mm:ss");
        final java.util.Date utilTime = sdf.parse(rawValue);
        return new Time(utilTime.getTime());
    }

}