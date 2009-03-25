package org.drizzle.jdbc.internal;

import java.sql.Time;
import java.sql.Date;
import java.sql.Blob;
import java.math.BigDecimal;


/**
 * User: marcuse
 * Date: Feb 23, 2009
 * Time: 10:42:02 PM

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

    public static DrizzleType fromServer(byte typeValue) {
        switch(typeValue) {
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
                return VIRTUAL;
            case 10:
                return NEWDECIMAL;
            case 11:
                return ENUM;
            case 12:
                return BLOB;
            case 13:
                return BLOB;
            default:
            return VARCHAR;
        }
        
    }
}
