// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.Connection;

public enum TransactionIsolation {
  REPEATABLE_READ("REPEATABLE-READ", java.sql.Connection.TRANSACTION_REPEATABLE_READ),
  READ_COMMITTED("READ-COMMITTED", Connection.TRANSACTION_READ_COMMITTED),
  READ_UNCOMMITTED("READ-UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED),
  SERIALIZABLE("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);

  private final String value;
  private final int level;

  TransactionIsolation(String value, int level) {
    this.value = value;
    this.level = level;
  }

  public String getValue() {
    return value;
  }

  public int getLevel() {
    return level;
  }

  public static TransactionIsolation from(String value) {
    for (TransactionIsolation transactionIsolation : values()) {
      if (transactionIsolation
          .value
          .replaceAll("[ \\-_]", "")
          .equalsIgnoreCase(value.replaceAll("[ \\-_]", ""))) {
        return transactionIsolation;
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for TransactionIsolation", value));
  }
}
