// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.export;

import java.sql.SQLException;

public class SQLPrepareException extends SQLException {
  public SQLPrepareException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
  }
}
