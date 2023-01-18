/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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

package org.mariadb.jdbc;

import java.sql.SQLException;
import java.util.Map;
import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.com.send.parameters.NullParameter;
import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.util.dao.CloneableCallableStatement;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionFactory;

public class MariaDbProcedureStatement extends CallableProcedureStatement
    implements CloneableCallableStatement {

  private SelectResultSet outputResultSet = null;

  /**
   * Specific implementation of CallableStatement to handle function call, represent by call like
   * {?= call procedure-name[(arg1,arg2, ...)]}.
   *
   * @param query query
   * @param connection current connection
   * @param procedureName procedure name
   * @param database database
   * @param resultSetType a result set type; one of <code>ResultSet.TYPE_FORWARD_ONLY</code>, <code>
   *     ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
   * @param resultSetConcurrency a concurrency type; one of <code>ResultSet.CONCUR_READ_ONLY</code>
   *     or <code>ResultSet.CONCUR_UPDATABLE</code>
   * @param exceptionFactory Exception Factory
   * @throws SQLException exception
   */
  public MariaDbProcedureStatement(
      String query,
      MariaDbConnection connection,
      String procedureName,
      String database,
      int resultSetType,
      int resultSetConcurrency,
      ExceptionFactory exceptionFactory)
      throws SQLException {
    super(
        connection,
        query,
        resultSetType,
        resultSetConcurrency,
        exceptionFactory,
        database,
        procedureName);
  }

  private void setInputOutputParameterMap() {
    if (outputParameterMapper == null) {
      int len = 0;
      if (serverPrepareResult == null) {
        for (Integer key : params.keySet()) {
          if (key + 1 > len) len = key + 1;
        }
      } else {
        len = serverPrepareResult.getParamCount();
      }
      outputParameterMapper = new int[len];

      for (int i = 0; i < outputParameterMapper.length; i++) {
        outputParameterMapper[i] = -1;
      }
      int currentOutputMapper = 1;
      for (Map.Entry<Integer, CallParameter> entry : params.entrySet()) {
        outputParameterMapper[entry.getKey()] =
            entry.getValue().isOutput() ? currentOutputMapper++ : -1;
      }
    }
  }

  protected SelectResultSet getOutputResult() throws SQLException {
    if (outputResultSet == null) {
      if (fetchSize != 0) {
        results.loadFully(false, protocol);
        outputResultSet = results.getCallableResultSet();
        if (outputResultSet != null) {
          outputResultSet.next();
          return outputResultSet;
        }
      }
      throw new SQLException("No output result.");
    }
    return outputResultSet;
  }

  /**
   * Clone statement.
   *
   * @param connection connection
   * @return Clone statement.
   * @throws CloneNotSupportedException if any error occur.
   */
  public MariaDbProcedureStatement clone(MariaDbConnection connection)
      throws CloneNotSupportedException {
    MariaDbProcedureStatement clone = (MariaDbProcedureStatement) super.clone(connection);
    clone.outputResultSet = null;
    return clone;
  }

  private void retrieveOutputResult() throws SQLException {
    // resultSet will be just before last packet
    outputResultSet = results.getCallableResultSet();
    if (outputResultSet != null) {
      outputResultSet.next();
    }
  }

  public void setParameter(final int parameterIndex, final ParameterHolder holder)
      throws SQLException {
    if (params.containsKey(parameterIndex - 1)) {
      params.get(parameterIndex - 1).setInput(true);
    } else {
      CallParameter param = new CallParameter();
      param.setInput(true);
      params.put(parameterIndex - 1, param);
    }
    super.setParameter(parameterIndex, holder);
  }

  @Override
  public boolean execute() throws SQLException {
    lock.lock();
    try {
      validAllParameters();
      super.executeInternal(fetchSize);
      retrieveOutputResult();
      return results != null && results.getResultSet() != null;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Valid that all parameters are set.
   *
   * @throws SQLException if set parameters is not right
   */
  private void validAllParameters() throws SQLException {

    setInputOutputParameterMap();
    // Set value for OUT parameters
    for (Map.Entry<Integer, CallParameter> entry : params.entrySet()) {
      if (!entry.getValue().isInput()) {
        super.setParameter(entry.getKey() + 1, new NullParameter());
      }
    }
    validParameters();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    if (hasInOutParameters == null) {
      setParametersVariables();
    }
    if (!hasInOutParameters) {
      return super.executeBatch();
    } else {
      throw new SQLException("executeBatch not permit for procedure with output parameter");
    }
  }
}
