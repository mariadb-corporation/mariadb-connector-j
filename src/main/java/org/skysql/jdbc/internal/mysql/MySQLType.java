/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc.internal.mysql;

import org.skysql.jdbc.internal.common.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;


/**
 * User: marcuse Date: Feb 23, 2009 Time: 10:42:02 PM
 */
public class MySQLType implements DataType {
    private final Type type;

    public MySQLType(final Type type) {
        this.type = type;
    }


    public Class getJavaType() {
        return type.getDataType();
    }

    public int getSqlType() {
        return type.getSqlType();
    }

    public String getTypeName() {
        return type.name();
    }

    public Type getType() {
        return type;
    }


    public enum Type {
        DECIMAL(java.sql.Types.DECIMAL, Double.class),
        TINY(java.sql.Types.SMALLINT, Short.class),
        SHORT(java.sql.Types.SMALLINT, Short.class),
        LONG(java.sql.Types.INTEGER, Integer.class),
        FLOAT(java.sql.Types.FLOAT, Float.class),
        DOUBLE(java.sql.Types.DOUBLE, Double.class),
        NULL(java.sql.Types.NULL, null),
        TIMESTAMP(java.sql.Types.TIMESTAMP, Long.class),
        LONGLONG(java.sql.Types.BIGINT, BigInteger.class),
        INT24(java.sql.Types.INTEGER, Integer.class),
        DATETIME(java.sql.Types.DATE, Date.class),
        DATE(java.sql.Types.DATE, Date.class),
        TIME(java.sql.Types.TIME, Time.class),
        YEAR(java.sql.Types.SMALLINT, Short.class),
        BIT(java.sql.Types.BIT, Boolean.class),
        VARCHAR(java.sql.Types.VARCHAR, String.class),
        NEWDECIMAL(java.sql.Types.DECIMAL, BigDecimal.class),
        ENUM(java.sql.Types.VARCHAR, String.class),
        SET(java.sql.Types.VARCHAR, String.class),
        BLOB(java.sql.Types.BLOB, Blob.class),
        MAX(java.sql.Types.BLOB, Blob.class),
        CLOB(java.sql.Types.CLOB, String.class);

        private final int sqlType;
        private final Class<?> javaClass;

        Type(final int sqlType, final Class<?> javaClass) {
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

    public static MySQLType fromServer(final byte typeValue) {        
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