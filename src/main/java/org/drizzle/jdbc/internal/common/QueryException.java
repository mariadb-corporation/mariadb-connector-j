/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 7, 2009
 * Time: 10:16:17 PM

 */
public class QueryException extends Exception {
    private final int errorCode;
    private final String sqlState;

/*    public QueryException(String message, Throwable cause) {
        super(message,cause);
        this.errorCode = -1;
        this.sqlState = "HY0000";
    }
*/
    public QueryException(String message) {
        super(message);
        this.errorCode = -1;
        this.sqlState = "HY0000";

    }

    public QueryException(String s, short errorCode, String sqlState) {
        super(s);
        this.errorCode=errorCode;
        this.sqlState=sqlState;
    }

    public QueryException(String s, int errorCode, String sqlState, Throwable cause) {
        super(s,cause);
        this.errorCode=errorCode;
        this.sqlState=sqlState;
   }

    public int getErrorCode() {
        return errorCode;
    }

    public String getSqlState() {
        return sqlState;
    }
}
