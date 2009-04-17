package org.drizzle.jdbc.internal.drizzle;

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
