package org.drizzle.jdbc.internal.queryresults;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:57:46 PM
 * To change this template use File | Settings | File Templates.
 */
public enum ColumnFlags {
    NOT_NULL((short)1),
    PRIMARY_KEY((short)2),
    UNIQUE_KEY((short)4),
    MULTIPLE_KEY((short)8),
    BLOB((short)16),
    UNSIGNED((short)32),
    DECIMAL((short)64),
    BINARY((short)128),
    ENUM((short)256),
    AUTO_INCREMENT((short)512),
    TIMESTAMP((short)1024),
    SET((short)2048);

    private short flag;

    ColumnFlags(short i) {
        this.flag=i;
    }

    public short flag() {
        return flag;
    }
}

