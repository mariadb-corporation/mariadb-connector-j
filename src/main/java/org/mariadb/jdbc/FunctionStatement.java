// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc;

import java.sql.*;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.client.util.Parameters;
import org.mariadb.jdbc.message.server.OkPacket;
import org.mariadb.jdbc.util.ParameterList;

/** Function callable statement implementation */
public class FunctionStatement extends BaseCallableStatement implements CallableStatement {
  /**
   * Constructor of function callable statement
   *
   * @param con current connection
   * @param databaseName database
   * @param procedureName procedure
   * @param arguments arguments
   * @param lock thread lock object
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
      ClosableLock lock,
      int resultSetType,
      int resultSetConcurrency)
      throws SQLException {
    super(
        "SELECT " + procedureName + arguments,
        con,
        lock,
        databaseName,
        procedureName,
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
    // the single row returned by "SELECT f(...)" is the return value, read as OUT parameter 1:
    // the call itself leaves no result set and no row count
    this.outputResultFromRes(1);
    currResult = OkPacket.BASIC_OK;
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
    // parameter 1 is the return value: the markers of "SELECT f(?)" start at parameter 2. The
    // shift only applies to this execution, the values stay in force for the next one
    Parameters all = parameters;
    Parameters shifted = new ParameterList(Math.max(0, all.size() - 1));
    for (int i = 1; i < all.size(); i++) {
      shifted.set(i - 1, all.get(i));
    }
    parameters = shifted;
    try {
      super.executeInternal();
    } finally {
      parameters = all;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FunctionStatement{sql:'" + sql + "'");
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
