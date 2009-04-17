package org.drizzle.jdbc.internal.common.queryresults;

import java.util.List;

/**
 .
 * User: marcuse
 * Date: Feb 5, 2009
 * Time: 10:20:03 PM

 */
public interface QueryResult {
    public ResultSetType getResultSetType();
     public void close();
    short getWarnings();
    String getMessage();

    List<ColumnInformation> getColumnInformation();

    int getRows();
}
