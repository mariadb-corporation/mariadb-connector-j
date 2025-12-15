// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.export;

import java.sql.DataTruncation;

public class MariaDbDataTruncation extends DataTruncation {
  private static final long serialVersionUID = 1L;

  private final String message;
  private final String sqlState;
  private final int errorCode;

  @SuppressWarnings("this-escape")
  public MariaDbDataTruncation(String message, String sqlState, int errorCode, Throwable cause) {
    super(-1, false, false, -1, -1);
    this.message = message;
    this.sqlState = sqlState;
    this.errorCode = errorCode;
    if (cause != null) {
      initCause(cause);
    }
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public String getSQLState() {
    return sqlState;
  }

  @Override
  public int getErrorCode() {
    return errorCode;
  }
}
