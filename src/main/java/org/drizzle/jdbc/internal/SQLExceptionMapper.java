package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.internal.common.QueryException;

import java.sql.*;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: May 20, 2009
 * Time: 5:06:56 PM
 * To change this template use File | Settings | File Templates.
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

        private String sqlStateGroup;


        SQLStates(String s) {
            this.sqlStateGroup = s;
        }

        public static SQLStates fromString(String group) {
            for (SQLStates state : SQLStates.values()) {
                if (group.startsWith(state.sqlStateGroup))
                    return state;
            }
            return UNDEFINED_SQLSTATE;
        }

        public String getSqlState() {
            return sqlStateGroup;
        }
    }

    public static SQLException get(QueryException e) {
        String sqlState = e.getSqlState();
        SQLStates state = SQLStates.fromString(sqlState);
        switch (state) {
            case DATA_EXCEPTION:
                return new SQLDataException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case FEATURE_NOT_SUPPORTED:
                return new SQLFeatureNotSupportedException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case CONSTRAINT_VIOLATION:
                return new SQLIntegrityConstraintViolationException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case INVALID_AUTHORIZATION:
                return new SQLInvalidAuthorizationSpecException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case CONNECTION_EXCEPTION:
                // TODO: check transient / non transient
                return new SQLNonTransientConnectionException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case SYNTAX_ERROR_ACCESS_RULE:
                return new SQLSyntaxErrorException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case TRANSACTION_ROLLBACK:
                return new SQLTransactionRollbackException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
            case WARNING:
                return new SQLWarning(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
        }
        return new SQLException(e.getMessage(), e.getMessage(), e.getErrorCode(), e);
    }
}
