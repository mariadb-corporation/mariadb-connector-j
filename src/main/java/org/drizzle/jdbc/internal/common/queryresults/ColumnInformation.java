package org.drizzle.jdbc.internal.common.queryresults;

import org.drizzle.jdbc.internal.drizzle.DrizzleType;

import java.util.Set;

/**
 .
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:55:30 PM

 */
public interface ColumnInformation {
    String getCatalog();
    String getDb();
    String getTable();
    String getOriginalTable();
    String getName();
    String getOriginalName();
    short getCharsetNumber();
    long getLength();
    DrizzleType getType();
    byte getDecimals();
    Set<ColumnFlags> getFlags();

    void updateDisplaySize(int displayLength);
}
