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
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.exceptions.ExceptionFactory;

public class BaseContext implements Context {

  private final long threadId;
  private final long serverCapabilities;
  private final byte[] seed;
  private final ServerVersion version;
  private final boolean eofDeprecated;
  private final boolean skipMeta;
  private final boolean extendedInfo;
  private final Configuration conf;
  private ExceptionFactory exceptionFactory;
  protected int serverStatus;
  private String database;
  private int transactionIsolationLevel;
  private int warning;
  private PrepareCache prepareCache;
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
    this.version = new ServerVersion(handshake.getServerVersion(), handshake.isMariaDBServer());
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
