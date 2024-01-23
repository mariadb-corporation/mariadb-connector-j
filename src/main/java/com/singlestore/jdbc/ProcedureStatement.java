// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

public class ProcedureStatement extends BaseCallableStatement implements CallableStatement {

  /**
   * Constructor
   *
   * @param con connection
   * @param sql sql
   * @param databaseName database
   * @param procedureName procedure
   * @param lock thread locker
   * @param canCachePrepStmts can cache server prepared result
   * @param resultSetType result-set type
   * @param resultSetConcurrency concurrency
   * @throws SQLException if any exception occurs
   */
  public ProcedureStatement(
      Connection con,
      String sql,
      String databaseName,
      String procedureName,
      ReentrantLock lock,
      boolean canCachePrepStmts,
      int resultSetType,
      int resultSetConcurrency)
      throws SQLException {
    super(
        sql,
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
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ProcedureStatement{sql:'" + sql + "'");
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
