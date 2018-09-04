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

package org.mariadb.jdbc.internal.util;

import static org.mariadb.jdbc.internal.util.SqlStates.CONNECTION_EXCEPTION;

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;

public class LogQueryTool {
    private final Options options;

    public LogQueryTool(Options options) {
        this.options = options;
    }

    /**
     * Get query, truncated if to big.
     *
     * @param sql current query
     * @return possibly truncated query if too big
     */
    public String subQuery(String sql) {
        if (options.maxQuerySizeToLog > 0 && sql.length() > options.maxQuerySizeToLog - 3) {
            return sql.substring(0, options.maxQuerySizeToLog - 3) + "...";
        }
        return sql;
    }

    /**
     * Get query, truncated if to big.
     *
     * @param buffer current query buffer
     * @return possibly truncated query if too big
     */
    private String subQuery(ByteBuffer buffer) {
        String queryString;
        if (options.maxQuerySizeToLog == 0) {
            queryString = new String(buffer.array(), 5, buffer.limit());
        } else {
            queryString = new String(buffer.array(), 5, Math.min(buffer.limit() - 5, (options.maxQuerySizeToLog * 3)));
            if (queryString.length() > options.maxQuerySizeToLog - 3) {
                queryString = queryString.substring(0, options.maxQuerySizeToLog - 3) + "...";
            }
        }
        return queryString;
    }

    /**
     * Return exception with query information's.
     *
     * @param sql               current sql command
     * @param sqlException      current exception
     * @param explicitClosed    has connection been explicitly closed
     * @return exception with query information
     */
    public SQLException exceptionWithQuery(String sql, SQLException sqlException, boolean explicitClosed) {
        if (explicitClosed) {
            return new SQLException("Connection has explicitly been closed/aborted.\nQuery is: " + subQuery(sql), sqlException.getSQLState(),
                    sqlException.getErrorCode(), sqlException.getCause());
        }

        if (options.dumpQueriesOnException || sqlException.getErrorCode() == 1064) {
            return new SQLException(sqlException.getMessage()
                + "\nQuery is: " + subQuery(sql)
                + "\njava thread: " + Thread.currentThread().getName(),
                sqlException.getSQLState(),
                sqlException.getErrorCode(), sqlException.getCause());
        }
        return sqlException;
    }

    /**
     * Return exception with query information's.
     *
     * @param buffer            query buffer
     * @param sqlEx             current exception
     * @param explicitClosed    has connection been explicitly closed
     * @return exception with query information
     */
    public SQLException exceptionWithQuery(ByteBuffer buffer, SQLException sqlEx, boolean explicitClosed) {
        if (options.dumpQueriesOnException || sqlEx.getErrorCode() == 1064 || explicitClosed) {
            return exceptionWithQuery(subQuery(buffer), sqlEx, explicitClosed);
        }
        return sqlEx;
    }

    /**
     * Return exception with query information's.
     *
     * @param parameters          query parameters
     * @param sqlEx               current exception
     * @param serverPrepareResult prepare results
     * @return exception with query information
     */
    public SQLException exceptionWithQuery(ParameterHolder[] parameters, SQLException sqlEx, PrepareResult serverPrepareResult) {
        if (sqlEx.getCause() instanceof SocketTimeoutException) {
            return new SQLException("Connection timed out", CONNECTION_EXCEPTION.getSqlState(), sqlEx);
        }
        if (options.dumpQueriesOnException) {
            return new SQLException(exWithQuery(sqlEx.getMessage(), serverPrepareResult, parameters), sqlEx.getSQLState(),
                    sqlEx.getErrorCode(), sqlEx.getCause());
        }
        return sqlEx;
    }

    /**
     * Return exception with query information's.
     *
     * @param sqlEx         current exception
     * @param prepareResult prepare results
     * @return exception with query information
     */
    public SQLException exceptionWithQuery(SQLException sqlEx, PrepareResult prepareResult) {
        if (options.dumpQueriesOnException || sqlEx.getErrorCode() == 1064) {
            String querySql = prepareResult.getSql();
            String message = sqlEx.getMessage();
            if (options.maxQuerySizeToLog != 0 && querySql.length() > options.maxQuerySizeToLog - 3) {
                message += "\nQuery is: " + querySql.substring(0, options.maxQuerySizeToLog - 3) + "...";
            } else {
                message += "\nQuery is: " + querySql;
            }
            message += "\njava thread: " + Thread.currentThread().getName();
            return new SQLException(message, sqlEx.getSQLState(), sqlEx.getErrorCode(), sqlEx.getCause());
        }
        return sqlEx;
    }

    /**
     * Return exception message with query.
     *
     * @param message             current exception message
     * @param serverPrepareResult prepare result
     * @param parameters          query parameters
     * @return exception message with query
     */
    private String exWithQuery(String message, PrepareResult serverPrepareResult, ParameterHolder[] parameters) {
        if (options.dumpQueriesOnException) {
            StringBuilder sql = new StringBuilder(serverPrepareResult.getSql());
            if (serverPrepareResult.getParamCount() > 0) {
                sql.append(", parameters [");
                if (parameters.length > 0) {
                    for (int i = 0; i < Math.min(parameters.length, serverPrepareResult.getParamCount()); i++) {
                        sql.append(parameters[i].toString()).append(",");
                    }
                    sql = new StringBuilder(sql.substring(0, sql.length() - 1));
                }
                sql.append("]");
            }

            if (options.maxQuerySizeToLog != 0 && sql.length() > options.maxQuerySizeToLog - 3) {
                return message
                    + "\nQuery is: " + sql.substring(0, options.maxQuerySizeToLog - 3) + "..."
                    + "\njava thread: " + Thread.currentThread().getName();
            } else {
                return message
                    + "\nQuery is: " + sql
                    + "\njava thread: " + Thread.currentThread().getName();
            }
        }
        return message;
    }

}
