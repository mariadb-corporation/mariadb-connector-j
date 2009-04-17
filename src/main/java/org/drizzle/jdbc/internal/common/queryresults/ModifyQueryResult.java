package org.drizzle.jdbc.internal.common.queryresults;

/**
 .
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 9:51:19 PM

 */
public interface ModifyQueryResult extends QueryResult {
 public long getUpdateCount() ;
    public short getWarnings() ;
    public String getMessage();
    public long getInsertId();
    public QueryResult getGeneratedKeysResult();     
}
