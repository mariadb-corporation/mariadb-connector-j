package org.drizzle.jdbc.exception;

import org.drizzle.jdbc.internal.common.QueryException;

import java.sql.SQLException;

public class SQLQueryTimedOutException extends SQLException {
    public SQLQueryTimedOutException(String message, String sqlState, int errorCode, QueryException e) {
        super(message, sqlState, errorCode, e);
    }
}
