// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.result.Result;

/** Procedure callable statement */
public class ProcedureStatement extends BaseCallableStatement implements CallableStatement {

  /**
   * Constructor
   *
   * @param con connection
   * @param sql sql
   * @param databaseName database
   * @param procedureName procedure
   * @param lock thread locker
   * @param canUseServerTimeout can use server timeout
   * @param canUseServerMaxRows can use server max rows
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
      boolean canUseServerTimeout,
      boolean canUseServerMaxRows,
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
        canUseServerTimeout,
        canUseServerMaxRows,
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
  protected void handleParameterOutput() throws SQLException {
    // output result-set is the last result-set
    // or in case finishing with an OK_PACKET, just the one before
    for (int i = 1; i <= Math.min(this.results.size(), 2); i++) {
      Completion compl = this.results.get(this.results.size() - i);
      if (compl instanceof Result && (((Result) compl).isOutputParameter())) {
        outputResultFromRes(i);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ProcedureStatement{sql:'" + sql + "'");
    sb.append(", parameters:[");
    for (int i = 0; i < parameters.size(); i++) {
      org.mariadb.jdbc.client.util.Parameter param = parameters.get(i);
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
