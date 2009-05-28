/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

/**
 * .
 * User: marcuse
 * Date: Feb 7, 2009
 * Time: 10:16:17 PM
 */
public class QueryException extends Exception {
    /**
     * the internal code.
     */
    private final int errorCode;
    /**
     * the sql state.
     */
    private final String sqlState;

    /**
     * Creates a default query exception with errorCode -1 and
     * sqlState HY0000.
     * @param message the message to set
     */
    public QueryException(final String message) {
        super(message);
        this.errorCode = -1;
        this.sqlState = "HY0000";

    }

    /**
     * Creates a query exception with a message.
     * @param message the message
     * @param errorCode the error code
     * @param sqlState the sqlstate
     */
    public QueryException(final String message,
                          final short errorCode,
                          final String sqlState) {
        super(message);
        this.errorCode = errorCode;
        this.sqlState = sqlState;
    }

    /**
     * creates a query exception with a message and a cause.
     * @param message the exception message
     * @param errorCode the error code
     * @param sqlState the sql state
     * @param cause the cause of the exception
     */
    public QueryException(final String message,
                          final int errorCode,
                          final String sqlState,
                          final Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.sqlState = sqlState;
    }

    /**
     * returns the error code.
     * @return the error code
     */
    public final int getErrorCode() {
        return errorCode;
    }

    /**
     * gets the sql state.
     * @return the sql state
     */
    public final String getSqlState() {
        return sqlState;
    }
}
