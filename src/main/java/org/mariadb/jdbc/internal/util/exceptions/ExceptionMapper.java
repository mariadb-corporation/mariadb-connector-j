/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.util.exceptions;

import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbStatement;
import org.mariadb.jdbc.internal.util.SqlStates;

import java.sql.*;

import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;


public class ExceptionMapper {

    /**
     * Helper to throw exception.
     *
     * @param exception  exception
     * @param connection current connection
     * @param statement  current statement
     * @throws SQLException exception
     */
    public static void throwException(SQLException exception, MariaDbConnection connection, MariaDbStatement statement) throws SQLException {
        throw getException(exception, connection, statement, false);
    }

    public static SQLException connException(String message) {
        return connException(message, null);
    }

    public static SQLException connException(String message, Throwable cause) {
        return get(message, CONNECTION_EXCEPTION.getSqlState(), -1, cause, false);
    }


    /**
     * Helper to decorate exception with associate subclass of {@link SQLException} exception.
     *
     * @param exception  exception
     * @param connection current connection
     * @param statement  current statement
     * @param timeout    was timeout on query
     * @return SQLException exception
     */
    public static SQLException getException(SQLException exception, MariaDbConnection connection,
                                            MariaDbStatement statement, boolean timeout) {
        String message = exception.getMessage();
        if (connection != null) {
            message = "(conn=" + connection.getServerThreadId() + ") " + message;
        } else if (statement != null) {
            message = "(conn=" + statement.getServerThreadId() + ") " + message;
        }

        SQLException sqlException;
        SqlStates state = null;

        if (exception.getSQLState() != null) {
            if (message.contains("\n")) message = message.substring(0, message.indexOf("\n"));
            sqlException = get(message, exception.getSQLState(), exception.getErrorCode(), exception, timeout);
            String sqlState = exception.getSQLState();
            state = SqlStates.fromString(sqlState);
            SQLException nextException = exception.getNextException();
            if (nextException != null) {
                sqlException.setNextException(nextException);
            }
        } else {
            sqlException = exception;
        }

        if (connection != null) {
            if (SqlStates.CONNECTION_EXCEPTION.equals(state)) {
                connection.setHostFailed();
                if (connection.pooledConnection != null) {
                    connection.pooledConnection.fireConnectionErrorOccured(sqlException);
                }
            } else if (connection.pooledConnection != null && statement != null) {
                connection.pooledConnection.fireStatementErrorOccured(statement, sqlException);
            }
        }

        return sqlException;
    }

    /**
     * Check connection exception to report to poolConnection listeners.
     *
     * @param exception current exception
     * @param connection current connection
     */
    public static void checkConnectionException(SQLException exception, MariaDbConnection connection) {
        if (exception.getSQLState() != null) {
            SqlStates state = SqlStates.fromString(exception.getSQLState());
            if (SqlStates.CONNECTION_EXCEPTION.equals(state)) {
                connection.setHostFailed();
                if (connection.pooledConnection != null) {
                    connection.pooledConnection.fireConnectionErrorOccured(exception);
                }
            }
        }
    }

    /**
     * Helper to decorate exception with associate subclass of {@link SQLException} exception.
     *
     * @param message       exception message
     * @param sqlState      sqlstate
     * @param errorCode     errorCode
     * @param exception     cause
     * @param timeout       was timeout on query
     * @return SQLException exception
     */
    public static SQLException get(final String message, String sqlState, int errorCode, final Throwable exception, boolean timeout) {
        final SqlStates state = SqlStates.fromString(sqlState);
        switch (state) {
            case DATA_EXCEPTION:
                return new SQLDataException(message, sqlState, errorCode, exception);
            case FEATURE_NOT_SUPPORTED:
                return new SQLFeatureNotSupportedException(message, sqlState, errorCode, exception);
            case CONSTRAINT_VIOLATION:
                return new SQLIntegrityConstraintViolationException(message, sqlState, errorCode, exception);
            case INVALID_AUTHORIZATION:
                return new SQLInvalidAuthorizationSpecException(message, sqlState, errorCode, exception);
            case CONNECTION_EXCEPTION:
                return new SQLNonTransientConnectionException(message, sqlState, errorCode, exception);
            case SYNTAX_ERROR_ACCESS_RULE:
                return new SQLSyntaxErrorException(message, sqlState, errorCode, exception);
            case TRANSACTION_ROLLBACK:
                return new SQLTransactionRollbackException(message, sqlState, errorCode, exception);
            case WARNING:
                return new SQLWarning(message, sqlState, errorCode, exception);
            case INTERRUPTED_EXCEPTION:
                if (timeout && "70100".equals(sqlState)) {
                    return new SQLTimeoutException(message, sqlState, errorCode, exception);
                }
                if (exception instanceof SQLNonTransientConnectionException) {
                    return new SQLNonTransientConnectionException(message, sqlState, errorCode, exception);
                }
                return new SQLTransientException(message, sqlState, errorCode, exception);
            case TIMEOUT_EXCEPTION:
                return new SQLTimeoutException(message, sqlState, errorCode, exception);
            case UNDEFINED_SQLSTATE:
                if (exception instanceof SQLNonTransientConnectionException) {
                    return new SQLNonTransientConnectionException(message, sqlState, errorCode, exception);
                }
            default:
                // DISTRIBUTED_TRANSACTION_ERROR,
                return new SQLException(message, sqlState, errorCode, exception);
        }
    }

    public static SQLException getSqlException(String message, Exception exception) {
        return new SQLException(message, exception);
    }

    public static SQLException getSqlException(String message, String sqlState, Exception exception) {
        return new SQLException(message, sqlState, 0, exception);
    }

    public static SQLException getSqlException(String message) {
        return new SQLException(message);
    }

    public static SQLException getFeatureNotSupportedException(String message) {
        return new SQLFeatureNotSupportedException(message);
    }

    /**
     * Mapp code to State.
     *
     * @param code code
     * @return String
     */
    public static String mapCodeToSqlState(int code) {
        switch (code) {
            case 1022: //ER_DUP_KEY
                return "23000";
            case 1037: //ER_OUTOFMEMORY
                return "HY001";
            case 1038: //ER_OUT_OF_SORTMEMORY
                return "HY001";
            case 1040: //ER_CON_COUNT_ERROR
                return "08004";
            case 1042: //ER_BAD_HOST_ERROR
                return "08S01";
            case 1043: //ER_HANDSHAKE_ERROR
                return "08S01";
            case 1044: //ER_DBACCESS_DENIED_ERROR
                return "42000";
            case 1045: //ER_ACCESS_DENIED_ERROR
                return "28000";
            case 1047: //ER_UNKNOWN_COM_ERROR
                return "HY000";
            case 1050: //ER_TABLE_EXISTS_ERROR
                return "42S01";
            case 1051: //ER_BAD_TABLE_ERROR
                return "42S02";
            case 1052: //ER_NON_UNIQ_ERROR
                return "23000";
            case 1053: //ER_SERVER_SHUTDOWN
                return "08S01";
            case 1054: //ER_BAD_FIELD_ERROR
                return "42S22";
            case 1055: //ER_WRONG_FIELD_WITH_GROUP
                return "42000";
            case 1056: //ER_WRONG_GROUP_FIELD
                return "42000";
            case 1057: //ER_WRONG_SUM_SELECT
                return "42000";
            case 1058: //ER_WRONG_VALUE_COUNT
                return "21S01";
            case 1059: //ER_TOO_LONG_IDENT
                return "42000";
            case 1060: //ER_DUP_FIELDNAME
                return "42S21";
            case 1061: //ER_DUP_KEYNAME
                return "42000";
            case 1062: //ER_DUP_ENTRY
                return "23000";
            case 1063: //ER_WRONG_FIELD_SPEC
                return "42000";
            case 1064: //ER_PARSE_ERROR
                return "42000";
            case 1065: //ER_EMPTY_QUERY
                return "42000";
            case 1066: //ER_NONUNIQ_TABLE
                return "42000";
            case 1067: //ER_INVALID_DEFAULT
                return "42000";
            case 1068: //ER_MULTIPLE_PRI_KEY
                return "42000";
            case 1069: //ER_TOO_MANY_KEYS
                return "42000";
            case 1070: //ER_TOO_MANY_KEY_PARTS
                return "42000";
            case 1071: //ER_TOO_LONG_KEY
                return "42000";
            case 1072: //ER_KEY_COLUMN_DOES_NOT_EXITS
                return "42000";
            case 1073: //ER_BLOB_USED_AS_KEY
                return "42000";
            case 1074: //ER_TOO_BIG_FIELDLENGTH
                return "42000";
            case 1075: //ER_WRONG_AUTO_KEY
                return "42000";
            case 1080: //ER_FORCING_CLOSE
                return "08S01";
            case 1081: //ER_IPSOCK_ERROR
                return "08S01";
            case 1082: //ER_NO_SUCH_INDEX
                return "42S12";
            case 1083: //ER_WRONG_FIELD_TERMINATORS
                return "42000";
            case 1084: //ER_BLOBS_AND_NO_TERMINATED
                return "42000";
            case 1090: //ER_CANT_REMOVE_ALL_FIELDS
                return "42000";
            case 1091: //ER_CANT_DROP_FIELD_OR_KEY
                return "42000";
            case 1101: //ER_BLOB_CANT_HAVE_DEFAULT
                return "42000";
            case 1102: //ER_WRONG_DB_NAME
                return "42000";
            case 1103: //ER_WRONG_TABLE_NAME
                return "42000";
            case 1104: //ER_TOO_BIG_SELECT
                return "42000";
            case 1106: //ER_UNKNOWN_PROCEDURE
                return "42000";
            case 1107: //ER_WRONG_PARAMCOUNT_TO_PROCEDURE
                return "42000";
            case 1109: //ER_UNKNOWN_TABLE
                return "42S02";
            case 1110: //ER_FIELD_SPECIFIED_TWICE
                return "42000";
            case 1112: //ER_UNSUPPORTED_EXTENSION
                return "42000";
            case 1113: //ER_TABLE_MUST_HAVE_COLUMNS
                return "42000";
            case 1115: //ER_UNKNOWN_CHARACTER_SET
                return "42000";
            case 1118: //ER_TOO_BIG_ROWSIZE
                return "42000";
            case 1120: //ER_WRONG_OUTER_JOIN
                return "42000";
            case 1121: //ER_NULL_COLUMN_IN_INDEX
                return "42000";
            case 1129: //ER_HOST_IS_BLOCKED
                return "HY000";
            case 1130: //ER_HOST_NOT_PRIVILEGED
                return "HY000";
            case 1131: //ER_PASSWORD_ANONYMOUS_USER
                return "42000";
            case 1132: //ER_PASSWORD_NOT_ALLOWED
                return "42000";
            case 1133: //ER_PASSWORD_NO_MATCH
                return "42000";
            case 1136: //ER_WRONG_VALUE_COUNT_ON_ROW
                return "21S01";
            case 1138: //ER_INVALID_USE_OF_NULL
                return "42000";
            case 1139: //ER_REGEXP_ERROR
                return "42000";
            case 1140: //ER_MIX_OF_GROUP_FUNC_AND_FIELDS
                return "42000";
            case 1141: //ER_NONEXISTING_GRANT
                return "42000";
            case 1142: //ER_TABLEACCESS_DENIED_ERROR
                return "42000";
            case 1143: //ER_COLUMNACCESS_DENIED_ERROR
                return "42000";
            case 1144: //ER_ILLEGAL_GRANT_FOR_TABLE
                return "42000";
            case 1145: //ER_GRANT_WRONG_HOST_OR_USER
                return "42000";
            case 1146: //ER_NO_SUCH_TABLE
                return "42S02";
            case 1147: //ER_NONEXISTING_TABLE_GRANT
                return "42000";
            case 1148: //ER_NOT_ALLOWED_COMMAND
                return "42000";
            case 1149: //ER_SYNTAX_ERROR
                return "42000";
            case 1152: //ER_ABORTING_CONNECTION
                return "08S01";
            case 1153: //ER_NET_PACKET_TOO_LARGE
                return "08S01";
            case 1154: //ER_NET_READ_ERROR_FROM_PIPE
                return "08S01";
            case 1155: //ER_NET_FCNTL_ERROR
                return "08S01";
            case 1156: //ER_NET_PACKETS_OUT_OF_ORDER
                return "08S01";
            case 1157: //ER_NET_UNCOMPRESS_ERROR
                return "08S01";
            case 1158: //ER_NET_READ_ERROR
                return "08S01";
            case 1159: //ER_NET_READ_INTERRUPTED
                return "08S01";
            case 1160: //ER_NET_ERROR_ON_WRITE
                return "08S01";
            case 1161: //ER_NET_WRITE_INTERRUPTED
                return "08S01";
            case 1162: //ER_TOO_LONG_STRING
                return "42000";
            case 1163: //ER_TABLE_CANT_HANDLE_BLOB
                return "42000";
            case 1164: //ER_TABLE_CANT_HANDLE_AUTO_INCREMENT
                return "42000";
            case 1166: //ER_WRONG_COLUMN_NAME
                return "42000";
            case 1167: //ER_WRONG_KEY_COLUMN
                return "42000";
            case 1169: //ER_DUP_UNIQUE
                return "23000";
            case 1170: //ER_BLOB_KEY_WITHOUT_LENGTH
                return "42000";
            case 1171: //ER_PRIMARY_CANT_HAVE_NULL
                return "42000";
            case 1172: //ER_TOO_MANY_ROWS
                return "42000";
            case 1173: //ER_REQUIRES_PRIMARY_KEY
                return "42000";
            case 1177: //ER_CHECK_NO_SUCH_TABLE
                return "42000";
            case 1178: //ER_CHECK_NOT_IMPLEMENTED
                return "42000";
            case 1179: //ER_CANT_DO_THIS_DURING_AN_TRANSACTION
                return "25000";
            case 1184: //ER_NEW_ABORTING_CONNECTION
                return "08S01";
            case 1189: //ER_MASTER_NET_READ
                return "08S01";
            case 1190: //ER_MASTER_NET_WRITE
                return "08S01";
            case 1203: //ER_TOO_MANY_USER_CONNECTIONS
                return "42000";
            case 1205: //ER_LOCK_WAIT_TIMEOUT
                return "41000";
            case 1207: //ER_READ_ONLY_TRANSACTION
                return "25000";
            case 1211: //ER_NO_PERMISSION_TO_CREATE_USER
                return "42000";
            case 1213: //ER_LOCK_DEADLOCK
                return "40001";
            case 1216: //ER_NO_REFERENCED_ROW
                return "23000";
            case 1217: //ER_ROW_IS_REFERENCED
                return "23000";
            case 1218: //ER_CONNECT_TO_MASTER
                return "08S01";
            case 1222: //ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT
                return "21000";
            case 1226: //ER_USER_LIMIT_REACHED
                return "42000";
            case 1230: //ER_NO_DEFAULT
                return "42000";
            case 1231: //ER_WRONG_VALUE_FOR_VAR
                return "42000";
            case 1232: //ER_WRONG_TYPE_FOR_VAR
                return "42000";
            case 1234: //ER_CANT_USE_OPTION_HERE
                return "42000";
            case 1235: //ER_NOT_SUPPORTED_YET
                return "42000";
            case 1239: //ER_WRONG_FK_DEF
                return "42000";
            case 1241: //ER_OPERAND_COLUMNS
                return "21000";
            case 1242: //ER_SUBQUERY_NO_1_ROW
                return "21000";
            case 1247: //ER_ILLEGAL_REFERENCE
                return "42S22";
            case 1248: //ER_DERIVED_MUST_HAVE_ALIAS
                return "42000";
            case 1249: //ER_SELECT_REDUCED
                return "01000";
            case 1250: //ER_TABLENAME_NOT_ALLOWED_HERE
                return "42000";
            case 1251: //ER_NOT_SUPPORTED_AUTH_MODE
                return "08004";
            case 1252: //ER_SPATIAL_CANT_HAVE_NULL
                return "42000";
            case 1253: //ER_COLLATION_CHARSET_MISMATCH
                return "42000";
            case 1261: //ER_WARN_TOO_FEW_RECORDS
                return "01000";
            case 1262: //ER_WARN_TOO_MANY_RECORDS
                return "01000";
            case 1263: //ER_WARN_NULL_TO_NOTNULL
                return "01000";
            case 1264: //ER_WARN_DATA_OUT_OF_RANGE
                return "01000";
            case 1265: //ER_WARN_DATA_TRUNCATED
                return "01000";
            case 1280: //ER_WRONG_NAME_FOR_INDEX
                return "42000";
            case 1281: //ER_WRONG_NAME_FOR_CATALOG
                return "42000";
            case 1286: //ER_UNKNOWN_STORAGE_ENGINE
                return "42000";
            default:
                return null;
        }
    }


}
