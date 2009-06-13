/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.mysql;

import org.drizzle.jdbc.internal.common.DataType;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;


/**
 * User: marcuse
 * Date: Feb 23, 2009
 * Time: 10:42:02 PM
 */
public class MySQLType implements DataType {
    private final Type type;

    public MySQLType(Type type) {
        this.type = type;
    }

    public Class getJavaType() {
        return type.getDataType();
    }

    public int getSqlType() {
        return type.getSqlType();
    }

    public Type getType() {
        return type;
    }


    public enum Type  {
        DECIMAL(java.sql.Types.DECIMAL, Double.class),
        TINY(java.sql.Types.SMALLINT, Short.class),
        SHORT(java.sql.Types.SMALLINT, Short.class),        
        LONG(java.sql.Types.BIGINT, Long.class),
        FLOAT(java.sql.Types.FLOAT, Float.class),
        DOUBLE(java.sql.Types.DOUBLE, Double.class),
        NULL(java.sql.Types.NULL, null),
        TIMESTAMP(java.sql.Types.TIMESTAMP, Long.class),
        LONGLONG(java.sql.Types.BIGINT, Long.class),
        INT24(java.sql.Types.INTEGER, Integer.class),
        DATETIME(java.sql.Types.DATE, Date.class),
        DATE(java.sql.Types.DATE, Date.class),
        TIME(java.sql.Types.TIME, Time.class),
        YEAR(java.sql.Types.SMALLINT, Short.class),
        BIT(java.sql.Types.BIT, Byte.class),
        VARCHAR(java.sql.Types.VARCHAR, String.class),
        NEWDECIMAL(java.sql.Types.DECIMAL, BigDecimal.class),
        ENUM(java.sql.Types.VARCHAR, String.class),
        SET(java.sql.Types.VARCHAR, String.class),
        BLOB(java.sql.Types.BLOB, Blob.class),
        MAX(java.sql.Types.BLOB, Blob.class);

        private int sqlType;
        private Class<?> javaClass;

        Type(int sqlType, Class<?> javaClass) {
            this.sqlType = sqlType;
            this.javaClass = javaClass;
        }

        public Class getDataType() {
            return javaClass;
        }

        public int getSqlType() {
            return sqlType;
        }
    }

    public static MySQLType fromServer(byte typeValue) {
        switch (typeValue) {
            case 0:
                return new MySQLType(Type.DECIMAL);
            case 1:
                return new MySQLType(Type.TINY);
            case 2:
                return new MySQLType(Type.SHORT);
            case 3:
                return new MySQLType(Type.LONG);
            case 4:
                return new MySQLType(Type.FLOAT);
            case 5:
                return new MySQLType(Type.DOUBLE);
            case 6:
                return new MySQLType(Type.NULL);
            case 7:
                return new MySQLType(Type.TIMESTAMP);
            case 8:
                return new MySQLType(Type.LONGLONG);
            case 9:
                return new MySQLType(Type.INT24);
            case 10:
                return new MySQLType(Type.DATE);
            case 11:
                return new MySQLType(Type.TIME);
            case 12:
                return new MySQLType(Type.DATETIME);
            case 13:
                return new MySQLType(Type.YEAR);
            case 14:
                return new MySQLType(Type.DATE);
            case 15:
                return new MySQLType(Type.VARCHAR);
            case 16:
                return new MySQLType(Type.BIT);
            case (byte) 0xf6:
                return new MySQLType(Type.NEWDECIMAL);
            case (byte) 0xf7:
                return new MySQLType(Type.ENUM);
            case (byte) 0xf8:
                return new MySQLType(Type.SET);
            case (byte) 0xf9:
            case (byte) 0xfa:
            case (byte) 0xfb:
            case (byte) 0xfc:
                return new MySQLType(Type.BLOB);
            default:
                return new MySQLType(Type.VARCHAR);
        }
    }
}