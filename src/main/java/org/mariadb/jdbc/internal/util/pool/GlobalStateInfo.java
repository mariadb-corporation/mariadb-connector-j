/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

package org.mariadb.jdbc.internal.util.pool;

import java.sql.*;

public class GlobalStateInfo {

  private final long maxAllowedPacket;
  private final int waitTimeout;
  private final boolean autocommit;
  private final int autoIncrementIncrement;
  private final String timeZone;
  private final String systemTimeZone;
  private final int defaultTransactionIsolation;

  /** Default value. ! To be used for Connection that will only Kill query/connection ! */
  public GlobalStateInfo() {
    this.maxAllowedPacket = 1_000_000;
    this.waitTimeout = 28_800;
    this.autocommit = true;
    this.autoIncrementIncrement = 1;
    this.timeZone = "+00:00";
    this.systemTimeZone = "+00:00";
    this.defaultTransactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;
  }

  /**
   * Storing global server state to avoid asking server each new connection. Using this Object
   * meaning having set the option "staticGlobal". Application must not change any of the following
   * options.
   *
   * @param maxAllowedPacket max_allowed_packet global variable value
   * @param waitTimeout wait_timeout global variable value
   * @param autocommit auto_commit global variable value
   * @param autoIncrementIncrement auto_increment_increment global variable value
   * @param timeZone time_zone global variable value
   * @param systemTimeZone System global variable value
   * @param defaultTransactionIsolation tx_isolation variable value
   */
  public GlobalStateInfo(
      long maxAllowedPacket,
      int waitTimeout,
      boolean autocommit,
      int autoIncrementIncrement,
      String timeZone,
      String systemTimeZone,
      int defaultTransactionIsolation) {
    this.maxAllowedPacket = maxAllowedPacket;
    this.waitTimeout = waitTimeout;
    this.autocommit = autocommit;
    this.autoIncrementIncrement = autoIncrementIncrement;
    this.timeZone = timeZone;
    this.systemTimeZone = systemTimeZone;
    this.defaultTransactionIsolation = defaultTransactionIsolation;
  }

  public long getMaxAllowedPacket() {
    return maxAllowedPacket;
  }

  public int getWaitTimeout() {
    return waitTimeout;
  }

  public boolean isAutocommit() {
    return autocommit;
  }

  public int getAutoIncrementIncrement() {
    return autoIncrementIncrement;
  }

  public String getTimeZone() {
    return timeZone;
  }

  public String getSystemTimeZone() {
    return systemTimeZone;
  }

  public int getDefaultTransactionIsolation() {
    return defaultTransactionIsolation;
  }
}
