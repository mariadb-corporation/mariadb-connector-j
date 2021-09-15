// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.client.context;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.client.PrepareCache;
import com.singlestore.jdbc.client.ServerVersion;
import com.singlestore.jdbc.message.server.InitialHandshakePacket;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.exceptions.ExceptionFactory;

public class BaseContext implements Context {

  private final long threadId;
  private final long serverCapabilities;
  private final byte[] seed;
  private final ServerVersion version;
  private final boolean eofDeprecated;
  private final boolean skipMeta;
  private final boolean extendedInfo;
  private final Configuration conf;
  private final ExceptionFactory exceptionFactory;
  protected int serverStatus;
  private String database;
  private int transactionIsolationLevel;
  private int warning;
  private final PrepareCache prepareCache;
  private int stateFlag = 0;

  public BaseContext(
      InitialHandshakePacket handshake,
      long clientCapabilities,
      Configuration conf,
      ExceptionFactory exceptionFactory,
      PrepareCache prepareCache) {
    this.threadId = handshake.getThreadId();
    this.seed = handshake.getSeed();
    this.serverCapabilities = handshake.getCapabilities();
    this.serverStatus = handshake.getServerStatus();
    this.version = handshake.getVersion();
    this.eofDeprecated = (clientCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0;
    this.skipMeta = (serverCapabilities & Capabilities.MARIADB_CLIENT_CACHE_METADATA) > 0;
    this.extendedInfo = (serverCapabilities & Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO) > 0;
    this.conf = conf;
    this.database = conf.database();
    this.exceptionFactory = exceptionFactory;
    this.prepareCache = prepareCache;
  }

  public long getThreadId() {
    return threadId;
  }

  public byte[] getSeed() {
    return seed;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public int getServerStatus() {
    return serverStatus;
  }

  public void setServerStatus(int serverStatus) {
    this.serverStatus = serverStatus;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public ServerVersion getVersion() {
    return version;
  }

  public boolean isEofDeprecated() {
    return eofDeprecated;
  }

  public boolean isExtendedInfo() {
    return extendedInfo;
  }

  public boolean canSkipMeta() {
    return skipMeta;
  }

  public int getWarning() {
    return warning;
  }

  public void setWarning(int warning) {
    this.warning = warning;
  }

  public ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  public Configuration getConf() {
    return conf;
  }

  public int getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  public void setTransactionIsolationLevel(int transactionIsolationLevel) {
    this.transactionIsolationLevel = transactionIsolationLevel;
  }

  public PrepareCache getPrepareCache() {
    return prepareCache;
  }

  public int getStateFlag() {
    return stateFlag;
  }

  public void resetStateFlag() {
    stateFlag = 0;
  }

  public void addStateFlag(int state) {
    stateFlag |= state;
  }
}
