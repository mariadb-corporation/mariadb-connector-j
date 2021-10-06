// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.client.result.Result;
import com.singlestore.jdbc.message.server.Completion;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;

public class ProcedureStatement extends BaseCallableStatement implements CallableStatement {

  public ProcedureStatement(
      Connection con,
      String sql,
      String databaseName,
      String procedureName,
      ReentrantLock lock,
      boolean canUseServerTimeout,
      boolean canUseServerMaxRows,
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
        this.outputResult = (Result) compl;
        this.outputResult.next();
        this.results.remove(this.results.size() - i);
      }
    }
  }
}
