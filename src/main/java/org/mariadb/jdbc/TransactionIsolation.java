// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.Connection;

/** Transaction isolation enumeration */
public enum TransactionIsolation {
  /**
   * dirty reads and non-repeatable reads are prevented; phantom reads can occur. This level
   * prohibits a transaction from reading a row with uncommitted changes in it, and it also
   * prohibits the situation where one transaction reads a row, a second transaction alters the row,
   * and the first transaction rereads the row, getting different values the second time (a
   * "non-repeatable read").
   */
  REPEATABLE_READ("REPEATABLE-READ", java.sql.Connection.TRANSACTION_REPEATABLE_READ),
  /**
   * dirty reads are prevented; non-repeatable reads and phantom reads can occur. This level only
   * prohibits a transaction from reading a row with uncommitted changes in it.
   */
  READ_COMMITTED("READ-COMMITTED", Connection.TRANSACTION_READ_COMMITTED),
  /**
   * dirty reads, non-repeatable reads and phantom reads can occur. This level allows a row changed
   * by one transaction to be read by another transaction before any changes in that row have been
   * committed (a "dirty read"). If any of the changes are rolled back, the second transaction will
   * have retrieved an invalid row.
   */
  READ_UNCOMMITTED("READ-UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED),
  /**
   * dirty reads, non-repeatable reads and phantom reads are prevented. This level includes the
   * prohibitions in TRANSACTION_REPEATABLE_READ and further prohibits the situation where one
   * transaction reads all rows that satisfy a WHERE condition, a second transaction inserts a row
   * that satisfies that WHERE condition, and the first transaction rereads for the same condition,
   * retrieving the additional "phantom" row in the second read.
   */
  SERIALIZABLE("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);

  private final String value;
  private final int level;

  TransactionIsolation(String value, int level) {
    this.value = value;
    this.level = level;
  }

  /**
   * Get TransactionIsolation from value
   *
   * @param value value
   * @return transaction isolation
   */
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

  /**
   * Get transaction isolation command
   *
   * @return transaction isolation command
   */
  public String getValue() {
    return value;
  }

  /**
   * Get transaction isolation level
   *
   * @return transaction isolation level
   */
  public int getLevel() {
    return level;
  }
}
