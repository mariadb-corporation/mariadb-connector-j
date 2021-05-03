// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.*;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.client.result.Result;
import org.mariadb.jdbc.util.ParameterList;

public class FunctionStatement extends BaseCallableStatement implements CallableStatement {

  public FunctionStatement(
      Connection con,
      String databaseName,
      String procedureName,
      String arguments,
      ReentrantLock lock,
      boolean canUseServerTimeout,
      boolean canUseServerMaxRows,
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
    this.outputResult = (Result) this.results.remove(this.results.size() - 1);
    this.outputResult.next();
  }

  @Override
  public void registerOutParameter(int index, int sqlType) throws SQLException {
    if (index <= 0 || index > 1) {
      throw exceptionFactory().create(String.format("wrong parameter index %s", index));
    }
    super.registerOutParameter(index, sqlType);
  }

  @Override
  protected void validParameters() throws SQLException {
    // remove first parameter, as it's an output param only
    ParameterList newParameters = new ParameterList(parameters.size() - 1);
    for (int i = 0; i < parameters.size() - 1; i++) {
      newParameters.set(i, parameters.get(i + 1));
    }
    parameters = newParameters;
    super.validParameters();
  }
}
