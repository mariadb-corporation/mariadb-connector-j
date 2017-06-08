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

package org.mariadb.jdbc.internal.logging;

import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.dao.ClientPrepareResult;
import org.mariadb.jdbc.internal.util.dao.PrepareResult;
import org.mariadb.jdbc.internal.util.dao.ServerPrepareResult;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;

public class ProtocolLoggingProxy implements InvocationHandler {
    private static final NumberFormat numberFormat = DecimalFormat.getInstance();
    private static Logger logger = LoggerFactory.getLogger(ProtocolLoggingProxy.class);
    protected boolean profileSql;
    protected Long slowQueryThresholdNanos;
    protected int maxQuerySizeToLog;
    protected Protocol protocol;

    /**
     * Constructor. Will create a proxy around protocol to log queries.
     *
     * @param protocol protocol to proxy
     * @param options  options
     */
    public ProtocolLoggingProxy(Protocol protocol, Options options) {
        this.protocol = protocol;
        this.profileSql = options.profileSql;
        this.slowQueryThresholdNanos = options.slowQueryThresholdNanos;
        this.maxQuerySizeToLog = options.maxQuerySizeToLog;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        long startTime = System.nanoTime();
        try {

            if ("executeQuery".equals(method.getName())
                || "executePreparedQuery".equals(method.getName())
                || "executeBatch".equals(method.getName())
                || "executeBatchMulti".equals(method.getName())
                || "executeBatchRewrite".equals(method.getName())
                || "executeBatchMultiple".equals(method.getName())
                || "prepareAndExecutes".equals(method.getName())
                || "prepareAndExecute".equals(method.getName())) {
                    Object returnObj = method.invoke(protocol, args);
                    if (logger.isInfoEnabled() && (profileSql
                            || (slowQueryThresholdNanos != null && System.nanoTime() - startTime > slowQueryThresholdNanos.longValue()))) {
                        logger.info("Query - conn:" + protocol.getServerThreadId() + "(" + (protocol.isMasterConnection() ? "M" : "S") + ")"
                                + " - " + numberFormat.format(((double) System.nanoTime() - startTime) / 1000000) + " ms"
                                + logQuery(method.getName(), args, returnObj));
                    }
                    return returnObj;
            }
            return method.invoke(protocol, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private String logQuery(String methodName, Object[] args, Object returnObj) {
        String sql = "";
        if ("executeQuery".equals(methodName)) {
            switch (args.length) {
                case 1:
                    sql = (String) args[0];
                    break;
                case 3:
                    sql = (String) args[2];
                    break;
                case 4:
                case 5:
                    if (Charset.class.isInstance(args[3])) {
                        sql = (String) args[2];
                        break;
                    }
                    ClientPrepareResult clientPrepareResult = (ClientPrepareResult) args[2];
                    sql = getQueryFromPrepareParameters(clientPrepareResult, (ParameterHolder[]) args[3], clientPrepareResult.getParamCount());
                    break;
                default:
                    //no default
            }
        } else if ("executeBatchMulti".equals(methodName)) {
            ClientPrepareResult clientPrepareResult = (ClientPrepareResult) args[2];
            sql = getQueryFromPrepareParameters(clientPrepareResult.getSql(), (List<ParameterHolder[]>) args[3],
                    clientPrepareResult.getParamCount());

        } else if ("executeBatch".equals(methodName)) {
            List<String> queries = (List<String>) args[2];
            for (int counter = 0; counter < queries.size(); counter++) {
                sql += queries.get(counter) + ";";
                if (maxQuerySizeToLog > 0 && sql.length() > maxQuerySizeToLog) break;
            }

        } else if ("executeBatchMultiple".equals(methodName)) {
            List<String> multipleQueries = (List<String>) args[2];
            if (multipleQueries.size() == 1) {
                sql = multipleQueries.get(0);
            } else {
                for (int counter = 0; counter < multipleQueries.size(); counter++) {
                    if (maxQuerySizeToLog > 0 && (sql.length() + multipleQueries.get(counter).length() + 1) > maxQuerySizeToLog) {
                        sql += multipleQueries.get(counter).substring(1, Math.max(1, maxQuerySizeToLog - sql.length()));
                        break;
                    }
                    sql += multipleQueries.get(counter) + ";";
                    if (maxQuerySizeToLog > 0 && sql.length() >= maxQuerySizeToLog) break;
                }
            }
        } else if ("prepareAndExecute".equals(methodName)) {
            ParameterHolder[] parameters = (ParameterHolder[]) args[4];
            ServerPrepareResult serverPrepareResult1 = (ServerPrepareResult) returnObj;
            sql = getQueryFromPrepareParameters(serverPrepareResult1, parameters, serverPrepareResult1.getParamCount());

        } else if ("prepareAndExecutes".equals(methodName)) {
            List<ParameterHolder[]> parameterList = (List<ParameterHolder[]>) args[4];
            ServerPrepareResult serverPrepareResult = (ServerPrepareResult) returnObj;
            sql = getQueryFromPrepareParameters(serverPrepareResult.getSql(), parameterList, serverPrepareResult.getParamCount());

        } else if ("executeBatchRewrite".equals(methodName)) {
            ClientPrepareResult prepareResultRewrite = (ClientPrepareResult) args[2];
            List<ParameterHolder[]> parameterListRewrite = (List<ParameterHolder[]>) args[3];
            sql = getQueryFromPrepareParameters(prepareResultRewrite.getSql(), parameterListRewrite, prepareResultRewrite.getParamCount());

        } else if ("executePreparedQuery".equals(methodName)) {
            ServerPrepareResult prepareResult = (ServerPrepareResult) args[1];
            if (args[3] instanceof ParameterHolder[]) {
                sql = getQueryFromPrepareParameters(prepareResult, (ParameterHolder[]) args[3], prepareResult.getParamCount());
            } else {
                sql = getQueryFromPrepareParameters(prepareResult.getSql(), (List<ParameterHolder[]>) args[3],
                        prepareResult.getParameters().length);
            }
        }

        if (maxQuerySizeToLog > 0) {
            return " - \"" + ((sql.length() < maxQuerySizeToLog) ? sql : sql.substring(0, maxQuerySizeToLog) + "...") + "\"";
        } else {
            return " - \"" + sql + "\"";
        }

    }

    private String getQueryFromPrepareParameters(String sql, List<ParameterHolder[]> parameterList, int parameterLength) {

        if (parameterLength == 0) {
            return sql;
        } else {
            StringBuilder sb = new StringBuilder(sql).append(", parameters ");
            for (int paramNo = 0; paramNo < parameterList.size(); paramNo++) {
                ParameterHolder[] parameters = parameterList.get(paramNo);

                if (paramNo != 0) sb.append(",");
                sb.append("[");
                for (int i = 0; i < parameterLength; i++) {
                    if (i != 0) sb.append(",");
                    sb.append(parameters[i].toString());
                }
                if (maxQuerySizeToLog > 0 && sb.length() > maxQuerySizeToLog) {
                    break;
                } else {
                    sb.append("]");
                }
            }
            return sb.toString();
        }
    }

    private String getQueryFromPrepareParameters(PrepareResult serverPrepareResult, ParameterHolder[] paramHolders, int parameterLength) {
        StringBuilder sb = new StringBuilder(serverPrepareResult.getSql());
        if (paramHolders.length > 0) {
            sb.append(", parameters [");
            for (int i = 0; i < parameterLength; i++) {
                if (i != 0) sb.append(",");
                sb.append(paramHolders[i].toString());
                if (maxQuerySizeToLog > 0 && sb.length() > maxQuerySizeToLog) break;
            }
            return sb.append("]").toString();
        }
        return serverPrepareResult.getSql();
    }

}
