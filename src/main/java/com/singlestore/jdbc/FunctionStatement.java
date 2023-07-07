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
  protected void validParameters() throws SQLException {
    // remove first parameter, as it's an output param only
    Parameters newParameters = new ParameterList(parameters.size() - 1);
    for (int i = 0; i < parameters.size() - 1; i++) {
      newParameters.set(i, parameters.get(i + 1));
    }
    parameters = newParameters;
    super.validParameters();
  }
}
