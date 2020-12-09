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

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.client.ServerVersion;
import org.mariadb.jdbc.client.result.Result;
import org.mariadb.jdbc.message.server.Completion;

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
    return true;
  }

  @Override
  protected void handleParameterOutput() throws SQLException {
    // resultset might be ended by an OK_packet
    // note. correction for MySQL 5.6 that wrongly remove PS_OUT_PARAMETERS flag of resultset.

    ServerVersion version = con.getContext().getVersion();
    Completion compl = this.results.get(this.results.size() - 1);
    if (compl instanceof Result
        && (((Result) compl).isOutputParameter()
            || (version.isMariaDBServer() && !version.versionGreaterOrEqual(10, 2, 0))
            || (!version.isMariaDBServer() && !version.versionGreaterOrEqual(5, 7, 0)))) {
      this.outputResult = (Result) compl;
      this.results.remove(this.results.size() - 1);
    } else if (this.results.size() > 1) {
      compl = this.results.get(this.results.size() - 2);
      if (compl instanceof Result
          && (((Result) compl).isOutputParameter()
              || (version.isMariaDBServer() && !version.versionGreaterOrEqual(10, 2, 0))
              || (!version.isMariaDBServer() && !version.versionGreaterOrEqual(5, 7, 0)))) {
        this.outputResult = (Result) compl;
        this.results.remove(this.results.size() - 2);
      }
    }
    this.outputResult.next();
  }
}
