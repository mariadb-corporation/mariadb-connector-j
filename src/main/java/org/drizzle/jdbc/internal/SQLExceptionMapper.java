package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: May 20, 2009 Time: 5:06:56 PM To change this template use File |
 * Settings | File Templates.
 */
public class SQLExceptionMapper {
    public enum SQLStates {
        WARNING("01"),
        NO_DATA("02"),
        CONNECTION_EXCEPTION("08"),
        FEATURE_NOT_SUPPORTED("0A"),
        CARDINALITY_VIOLATION("21"),
        DATA_EXCEPTION("22"),
        CONSTRAINT_VIOLATION("23"),
        INVALID_CURSOR_STATE("24"),
        INVALID_TRANSACTION_STATE("25"),
        INVALID_AUTHORIZATION("28"),
        SQL_FUNCTION_EXCEPTION("2F"),
        TRANSACTION_ROLLBACK("40"),
        SYNTAX_ERROR_ACCESS_RULE("42"),
        INVALID_CATALOG("3D"),
        INTERRUPTED_EXCEPTION("70"),
        UNDEFINED_SQLSTATE("HY"),
        DISTRIBUTED_TRANSACTION_ERROR("XA"); // is this true?

        private final String sqlStateGroup;


        SQLStates(final String s) {
            this.sqlStateGroup = s;
        }

        public static SQLStates fromString(final String group) {
            for (final SQLStates state : SQLStates.values()) {
                if (group.startsWith(state.sqlStateGroup)) {
                    return state;
                }
            }
            return UNDEFINED_SQLSTATE;
        }

        public String getSqlState() {
            return sqlStateGroup;
        }
    }

    public static SQLException get(final QueryException e) {
        final String sqlState = e.getSqlState();
        final SQLStates state = SQLStates.fromString(sqlState);
        if (Utils.isJava5()) {
            return new SQLException(e.getMessage(), sqlState, e.getErrorCode());
        } else {
            switch (state) {
                case DATA_EXCEPTION:
                    return new java.sql.SQLDataException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case FEATURE_NOT_SUPPORTED:
                    return new java.sql.SQLFeatureNotSupportedException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case CONSTRAINT_VIOLATION:
                    return new java.sql.SQLIntegrityConstraintViolationException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case INVALID_AUTHORIZATION:
                    return new java.sql.SQLInvalidAuthorizationSpecException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case CONNECTION_EXCEPTION:
                    // TODO: check transient / non transient
                    return new java.sql.SQLNonTransientConnectionException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case SYNTAX_ERROR_ACCESS_RULE:
                    return new java.sql.SQLSyntaxErrorException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case TRANSACTION_ROLLBACK:
                    return new java.sql.SQLTransactionRollbackException(e.getMessage(), sqlState, e.getErrorCode(), e);
                case WARNING:
                    return new SQLWarning(e.getMessage(), sqlState, e.getErrorCode(), e);
            }
            return new SQLException(e.getMessage(), sqlState, e.getErrorCode(), e);
        }
    }

    public static SQLException getSQLException(String message, Exception e) {
        if (Utils.isJava5()) {
            return new SQLException(message);
        } else {
            return new SQLException(message, e);
        }
    }

    public static SQLException getSQLException(String message) {
        return new SQLException(message);
    }

    public static SQLException getFeatureNotSupportedException(String message, Exception e) {
        if (Utils.isJava5()) {
            return new SQLException(message);
        } else {
            return new java.sql.SQLFeatureNotSupportedException(message, e);
        }
    }

    public static SQLException getFeatureNotSupportedException(String message) {
        if (Utils.isJava5()) {
            return new SQLException(message);
        } else {
            return new java.sql.SQLFeatureNotSupportedException(message);
        }
    }

}
