// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc;

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
   * @param canCachePrepStmts can cache server prepared result
   * @param resultSetType result set type
   * @param resultSetConcurrency concurrency type
   * @throws SQLException if any error occurs
   */
  @SuppressWarnings({"this-escape"})
  public FunctionStatement(
      Connection con,
      String databaseName,
      String procedureName,
      String arguments,
      ReentrantLock lock,
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
        canCachePrepStmts,
        resultSetType,
        resultSetConcurrency,
        0);
  }

  @Override
  public boolean isFunction() {
    return true;
  }

  @Override
  public void registerOutParameter(int index, int sqlType) throws SQLException {
    super.registerOutParameter(index, sqlType);
  }

  @Override
  protected void executeInternal() throws SQLException {
    super.executeInternal();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FunctionStatement{sql:'" + sql + "'");
    sb.append(", parameters:[");
    for (int i = 0; i < parameters.size(); i++) {
      com.singlestore.jdbc.client.util.Parameter param = parameters.get(i);
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
