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

package org.mariadb.jdbc.client.context;

import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.PrepareCache;
import org.mariadb.jdbc.client.ServerVersion;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public interface Context {

  long getThreadId();

  byte[] getSeed();

  long getServerCapabilities();

  int getServerStatus();

  void setServerStatus(int serverStatus);

  String getDatabase();

  void setDatabase(String database);

  ServerVersion getVersion();

  boolean isEofDeprecated();

  boolean canSkipMeta();

  boolean isExtendedInfo();

  int getWarning();

  void setWarning(int warning);

  ExceptionFactory getExceptionFactory();

  Configuration getConf();

  int getTransactionIsolationLevel();

  void setTransactionIsolationLevel(int transactionIsolationLevel);

  PrepareCache getPrepareCache();

  int getStateFlag();

  void resetStateFlag();

  void addStateFlag(int state);
}
