/*
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
 */

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
