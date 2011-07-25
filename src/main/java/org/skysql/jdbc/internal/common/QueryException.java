/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.skysql.jdbc.internal.common;

/**
 * . User: marcuse Date: Feb 7, 2009 Time: 10:16:17 PM
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
     * Creates a default query exception with errorCode -1 and sqlState HY0000.
     *
     * @param message the message to set
     */
    public QueryException(final String message) {
        super(message);
        this.errorCode = -1;
        this.sqlState = "HY0000";

    }

    /**
     * Creates a query exception with a message.
     *
     * @param message   the message
     * @param errorCode the error code
     * @param sqlState  the sqlstate
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
     *
     * @param message   the exception message
     * @param errorCode the error code
     * @param sqlState  the sql state
     * @param cause     the cause of the exception
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
     *
     * @return the error code
     */
    public final int getErrorCode() {
        return errorCode;
    }

    /**
     * gets the sql state.
     *
     * @return the sql state
     */
    public final String getSqlState() {
        return sqlState;
    }
}
