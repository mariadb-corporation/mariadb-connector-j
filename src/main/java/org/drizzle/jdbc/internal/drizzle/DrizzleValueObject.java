/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle;

import org.drizzle.jdbc.DrizzleBlob;
import org.drizzle.jdbc.internal.common.AbstractValueObject;
import org.drizzle.jdbc.internal.common.DataType;
import org.drizzle.jdbc.internal.mysql.MySQLType;

import java.text.ParseException;

/**
 * Contains the raw value returned from the server
 * <p/>
 * Is immutable
 * <p/>
 * User: marcuse Date: Feb 16, 2009 Time: 9:18:26 PM
 */
public class DrizzleValueObject extends AbstractValueObject {
    public DrizzleValueObject(final byte[] rawBytes, final DataType dataType) {
        super(rawBytes, dataType);
    }

    public Object getObject() throws ParseException {
        if (this.getBytes() == null) {
            return null;
        }
        switch (dataType.getType()) {
            case TINY:
                return getBoolean();
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
        }
        return null;
    }

}