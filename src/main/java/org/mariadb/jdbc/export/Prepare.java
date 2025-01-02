// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.export;

import java.sql.SQLException;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.ColumnDecoder;

/** Prepare packet COM_STMT_PREPARE (see https://mariadb.com/kb/en/com_stmt_prepare/) */
public interface Prepare {

  /**
   * Close Prepared command
   *
   * @param con current connection
   * @throws SQLException if prepare close fails
   */
  void close(Client con) throws SQLException;

  /**
   * Decrement use of prepare. In case not used anymore, and not in cache, will be close.
   *
   * @param con connection
   * @param preparedStatement current prepared statement that was using prepare object
   * @throws SQLException if close fails
   */
  void decrementUse(Client con, BasePreparedStatement preparedStatement) throws SQLException;

  /**
   * Get current prepare statement id
   *
   * @return statement id
   */
  int getStatementId();

  /**
   * Prepare parameters
   *
   * @return parameters metadata
   */
  ColumnDecoder[] getParameters();

  /**
   * Prepare result-set columns
   *
   * @return result-set columns metadata
   */
  ColumnDecoder[] getColumns();

  /**
   * set prepare result-set columns
   *
   * @param columns set result-set columns metadata
   */
  void setColumns(ColumnDecoder[] columns);
}
