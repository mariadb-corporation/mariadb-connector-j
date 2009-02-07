package org.drizzle.jdbc.internal;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 7, 2009
 * Time: 10:16:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryException extends Exception {
    public QueryException(String message, Throwable cause) {
        super(message,cause);
    }

    public QueryException(String message) {
        super(message);
    }
}
