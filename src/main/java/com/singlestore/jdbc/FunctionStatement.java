// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.client.util.Parameters;
import com.singlestore.jdbc.util.ParameterList;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

public class FunctionStatement extends BaseCallableStatement implements CallableStatement {

  /**
   * Constructor of function callable statement
   *
   * @param con current connection
   * @param databaseName database
   * @param procedureName procedure
   * @param arguments arguments
   * @param lock thread lock object
   * @param canUseServerTimeout can use server timeout
   * @param canUseServerMaxRows can use server max rows
   * @param canCachePrepStmts can cache server prepared result
   * @param resultSetType result set type
   * @param resultSetConcurrency concurrency type
   * @throws SQLException if any error occurs
   */
  public FunctionStatement(
      Connection con,
      String databaseName,
      String procedureName,
      String arguments,
      ReentrantLock lock,
      boolean canUseServerTimeout,
      boolean canUseServerMaxRows,
      boolean canCachePrepStmts,
      int resultSetType,
      int resultSetConcurrency)
      throws SQLException {
    super(
        "SELECT " + procedureName + arguments,
        con,
        lock,
        databaseName,
        procedureName,
        canUseServerTimeout,
        canUseServerMaxRows,
        canCachePrepStmts,
        resultSetType,
        resultSetConcurrency,
        0);
    registerOutParameter(1, null);
  }

  @Override
  public boolean isFunction() {
    return true;
  }

  @Override
  protected void handleParameterOutput() throws SQLException {
    this.outputResultFromRes(1);
  }

  @Override
  public void registerOutParameter(int index, int sqlType) throws SQLException {
    if (index != 1) {
      throw con.getExceptionFactory()
          .of(this)
          .create(String.format("wrong parameter index %s", index));
    }
    super.registerOutParameter(index, sqlType);
  }

  @Override
  protected void executeInternal() throws SQLException {
    preValidParameters();
    super.executeInternal();
  }

  /**
   * Ensures that returning value is not taken as a parameter.
   *
   * @throws SQLException if any exception
   */
  protected void preValidParameters() throws SQLException {
    // remove first parameter, as it's an output param only
    Parameters newParameters = new ParameterList(parameters.size() - 1);
    for (int i = 0; i < parameters.size() - 1; i++) {
      newParameters.set(i, parameters.get(i + 1));
    }
    parameters = newParameters;
    super.validParameters();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FunctionStatement{sql:'" + sql + "'");
    sb.append(", parameters:[");
    for (int i = 0; i < parameters.size(); i++) {
      com.singlestore.jdbc.client.util.Parameter param = parameters.get(i);
      if (outputParameters.contains(i + 1)) sb.append("<OUT>");
      if (param == null) {
        sb.append("null");
      } else {
        sb.append(param.bestEffortStringValue(con.getContext()));
      }
      if (i != parameters.size() - 1) {
        sb.append(",");
      }
    }
    sb.append("]}");
    return sb.toString();
  }
}
