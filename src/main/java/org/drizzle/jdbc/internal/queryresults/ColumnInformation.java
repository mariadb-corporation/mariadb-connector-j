package org.drizzle.jdbc.internal.queryresults;

import org.drizzle.jdbc.internal.DrizzleType;

import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 8:55:30 PM
 * To change this template use File | Settings | File Templates.
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
