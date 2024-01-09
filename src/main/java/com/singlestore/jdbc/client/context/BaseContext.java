// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.client.context;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.ColumnDecoder;
import com.singlestore.jdbc.client.Context;
import com.singlestore.jdbc.client.PrepareCache;
import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.export.ExceptionFactory;
import com.singlestore.jdbc.message.server.InitialHandshakePacket;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.ConnectionState;
import java.util.function.Function;

public class BaseContext implements Context {

  private final long serverCapabilities;
  private final long clientCapabilities;
  private final byte[] seed;
  private final boolean eofDeprecated;
  private final boolean skipMeta;
  private final Function<ReadableByteBuf, ColumnDecoder> columnDecoderFunction;
  private final Configuration conf;
  private final ExceptionFactory exceptionFactory;

  /** LRU prepare cache object */
  private final PrepareCache prepareCache;

  private final HostAddress hostAddress;
  /** Server status context */
  protected int serverStatus;

  private long threadId;
  private String charset;
  /** Server current database */
  private String database;
  /** Server current transaction isolation level */
  private Integer transactionIsolationLevel;
  /** Server current warning count */
  private int warning;
  /** Connection state use flag */
  private int stateFlag = 0;

  /**
   * Constructor of connection context
   *
   * @param hostAddress host address
   * @param handshake server handshake
   * @param clientCapabilities client capabilities
   * @param conf connection configuration
   * @param exceptionFactory connection exception factory
   * @param prepareCache LRU prepare cache
   */
  @SuppressWarnings({"this-escape"})
  public BaseContext(
      HostAddress hostAddress,
      InitialHandshakePacket handshake,
      long clientCapabilities,
      Configuration conf,
      ExceptionFactory exceptionFactory,
      PrepareCache prepareCache) {
    this.hostAddress = hostAddress;
    this.threadId = handshake.getThreadId();
    this.seed = handshake.getSeed();
    this.serverCapabilities = handshake.getCapabilities();
    this.clientCapabilities = clientCapabilities;
    this.serverStatus = handshake.getServerStatus();
    this.eofDeprecated = (clientCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0;
    this.skipMeta = (serverCapabilities & Capabilities.CACHE_METADATA) > 0;
    this.columnDecoderFunction =
        (serverCapabilities & Capabilities.EXTENDED_TYPE_INFO) > 0
            ? ColumnDecoder::decode
            : ColumnDecoder::decodeStd;
    this.conf = conf;
    this.database = conf.database();
    this.exceptionFactory = exceptionFactory;
    this.prepareCache = prepareCache;
  }

  public long getThreadId() {
    return threadId;
  }

  @Override
  public void setThreadId(long connectionId) {
    threadId = connectionId;
  }

  public byte[] getSeed() {
    return seed;
  }

  @Override
  public boolean hasServerCapability(long flag) {
    return (serverCapabilities & flag) > 0;
  }

  @Override
  public boolean hasClientCapability(long flag) {
    return (clientCapabilities & flag) > 0;
  }

  @Override
  public boolean permitPipeline() {
    return !conf.disablePipeline();
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

  public boolean isEofDeprecated() {
    return eofDeprecated;
  }

  public boolean canSkipMeta() {
    return skipMeta;
  }

  @Override
  public Function<ReadableByteBuf, ColumnDecoder> getColumnDecoderFunction() {
    return columnDecoderFunction;
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

  public Integer getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  public void setTransactionIsolationLevel(int transactionIsolationLevel) {
    this.addStateFlag(ConnectionState.STATE_TRANSACTION_ISOLATION);
    this.transactionIsolationLevel = transactionIsolationLevel;
  }

  public PrepareCache getPrepareCache() {
    return prepareCache;
  }

  @Override
  public void resetPrepareCache() {
    if (prepareCache != null) prepareCache.reset();
  }

  @Override
  public int getStateFlag() {
    return stateFlag;
  }

  @Override
  public void resetStateFlag() {
    stateFlag = 0;
  }

  @Override
  public void addStateFlag(int state) {
    stateFlag |= state;
  }

  @Override
  public void setTreadsConnected(long threadsConnected) {
    if (hostAddress != null) hostAddress.setThreadsConnected(threadsConnected);
  }

  public String getCharset() {
    return charset;
  }

  @Override
  public void setCharset(String charset) {
    this.charset = charset;
  }
}
