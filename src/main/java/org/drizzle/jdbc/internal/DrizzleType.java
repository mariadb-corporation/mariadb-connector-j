package org.drizzle.jdbc.internal;

import java.sql.Time;
import java.sql.Date;
import java.sql.Blob;
import java.math.BigDecimal;


/**
 * User: marcuse
 * Date: Feb 23, 2009
 * Time: 10:42:02 PM
 * To change this template use File | Settings | File Templates.
 */
public enum DrizzleType {
    TINY(java.sql.Types.SMALLINT,Short.class),
    LONG(java.sql.Types.BIGINT, Long.class),
    DOUBLE(java.sql.Types.DOUBLE, Double.class),
    NULL(java.sql.Types.NULL,null),
    TIMESTAMP(java.sql.Types.TIMESTAMP, Long.class),
    LONGLONG(java.sql.Types.BIGINT,Long.class),
    TIME(java.sql.Types.TIME, Time.class),
    DATETIME(java.sql.Types.DATE, Date.class),
    DATE(java.sql.Types.DATE,Date.class),
    VARCHAR(java.sql.Types.VARCHAR,String.class),
    VIRTUAL(java.sql.Types.OTHER,null),
    NEWDECIMAL(java.sql.Types.DECIMAL, BigDecimal.class),
    ENUM(java.sql.Types.VARCHAR,String.class),
    BLOB(java.sql.Types.BLOB, Blob.class),
    MAX(java.sql.Types.BLOB,Blob.class);

    private final int sqlType;
    private final Class javaClass;
    DrizzleType(int sqlType, Class clazz) {
        this.javaClass = clazz;
        this.sqlType=sqlType;
    }

    public int getSqlType() {
        return sqlType;
    }

    public Class getJavaClass() {
        return javaClass;
    }
}
