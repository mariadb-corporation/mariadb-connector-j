package org.drizzle.jdbc.internal.queryresults;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Mar 9, 2009
 * Time: 9:51:19 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ModifyQueryResult extends QueryResult {
 public long getUpdateCount() ;
    public short getWarnings() ;
    public String getMessage();
    public long getInsertId();
    public QueryResult getGeneratedKeysResult();     
}
