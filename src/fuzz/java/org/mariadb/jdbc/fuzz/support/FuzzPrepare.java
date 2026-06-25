// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import java.sql.SQLException;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.export.Prepare;

/** Unified mock for MariaDB prepared statement result. */
public class FuzzPrepare implements Prepare {
  private ColumnDecoder[] columns = new FuzzColumn[0];
  private final ColumnDecoder[] parameters;

  public FuzzPrepare(int parameterCount) {
    this.parameters = new FuzzColumn[parameterCount];
    for (int i = 0; i < parameterCount; i++) {
      this.parameters[i] = new FuzzColumn();
    }
  }

  @Override
  public int getStatementId() {
    return 1;
  }

  @Override
  public ColumnDecoder[] getParameters() {
    return parameters;
  }

  @Override
  public ColumnDecoder[] getColumns() {
    return columns;
  }

  @Override
  public void setColumns(ColumnDecoder[] columns) {
    this.columns = columns;
  }

  @Override
  public void close(Client con) throws SQLException {}

  @Override
  public void decrementUse(Client con, BasePreparedStatement preparedStatement) throws SQLException {}
}
