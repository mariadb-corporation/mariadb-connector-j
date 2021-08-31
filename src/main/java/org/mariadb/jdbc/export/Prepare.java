// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.export;

import java.sql.SQLException;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Column;

public interface Prepare {
  void close(Client con) throws SQLException;

  void decrementUse(Client con, ServerPreparedStatement preparedStatement) throws SQLException;

  int getStatementId();

  Column[] getParameters();

  Column[] getColumns();

  void setColumns(Column[] columns);
}
