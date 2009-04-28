/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

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
