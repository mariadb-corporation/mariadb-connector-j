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

package org.drizzle.jdbc.internal;

import org.drizzle.jdbc.exception.SQLQueryCancelledException;
import org.drizzle.jdbc.exception.SQLQueryTimedOutException;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.Utils;

import java.sql.SQLException;
import java.sql.SQLWarning;


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
        JAVA_SPECIFIC("JZ"),
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
                case JAVA_SPECIFIC:
                    if(sqlState.equals("JZ0001")) return new SQLQueryCancelledException(e.getMessage(), sqlState, e.getErrorCode(), e);
                    return new SQLQueryTimedOutException(e.getMessage(), sqlState, e.getErrorCode(), e);
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
