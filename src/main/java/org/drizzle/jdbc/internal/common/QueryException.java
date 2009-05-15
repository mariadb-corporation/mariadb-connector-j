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
    public QueryException(String message, Throwable cause) {
        super(message,cause);
    }

    public QueryException(String message) {
        super(message);
    }
}
