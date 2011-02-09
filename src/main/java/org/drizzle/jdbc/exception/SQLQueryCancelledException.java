package org.drizzle.jdbc.exception;

import org.drizzle.jdbc.internal.common.QueryException;

import java.sql.SQLException;

public class SQLQueryCancelledException extends SQLException {
    public SQLQueryCancelledException(String message) {
        super(message);
    }

    public SQLQueryCancelledException(String message, String sqlState, int errorCode, QueryException e) {
        super(message, sqlState, errorCode, e);
    }
}
