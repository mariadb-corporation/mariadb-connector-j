// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.context;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.PrepareCache;
import com.singlestore.jdbc.util.exceptions.ExceptionFactory;

public interface Context {

  long getThreadId();

  byte[] getSeed();

  long getServerCapabilities();

  int getServerStatus();

  void setServerStatus(int serverStatus);

  String getDatabase();

  void setDatabase(String database);

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
