/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;


/**
 * User: marcuse
 * Date: Feb 23, 2009
 * Time: 10:42:02 PM
 */
public enum DrizzleType {
    TINY(java.sql.Types.SMALLINT, Short.class),
    LONG(java.sql.Types.BIGINT, Long.class),
    DOUBLE(java.sql.Types.DOUBLE, Double.class),
    NULL(java.sql.Types.NULL, null),
    TIMESTAMP(java.sql.Types.TIMESTAMP, Long.class),
    LONGLONG(java.sql.Types.BIGINT, Long.class),
    DATETIME(java.sql.Types.DATE, Date.class),
    DATE(java.sql.Types.DATE, Date.class),
    VARCHAR(java.sql.Types.VARCHAR, String.class),
    NEWDECIMAL(java.sql.Types.DECIMAL, BigDecimal.class),
    ENUM(java.sql.Types.VARCHAR, String.class),
    BLOB(java.sql.Types.BLOB, Blob.class),
    MAX(java.sql.Types.BLOB, Blob.class);

    private final int sqlType;
    private final Class javaClass;

    DrizzleType(int sqlType, Class clazz) {
        this.javaClass = clazz;
        this.sqlType = sqlType;
    }

    public int getSqlType() {
        return sqlType;
    }

    public Class getJavaClass() {
        return javaClass;
    }

    public static DrizzleType fromServer(byte typeValue) {
        switch (typeValue) {
            case 0:
                return TINY;
            case 1:
                return LONG;
            case 2:
                return DOUBLE;
            case 3:
                return NULL;
            case 4:
                return TIMESTAMP;
            case 5:
                return LONGLONG;
            case 6:
                return DATETIME;
            case 7:
                return DATE;
            case 8:
                return VARCHAR;
            case 9:
                return NEWDECIMAL;
            case 10:
                return ENUM;
            case 11:
                return BLOB;
            case 12:
                return BLOB;
            default:
                return VARCHAR;
        }
    }
}
