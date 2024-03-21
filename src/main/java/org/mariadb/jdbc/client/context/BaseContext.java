// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.context;

import static org.mariadb.jdbc.util.constants.Capabilities.STMT_BULK_OPERATIONS;

import java.util.function.Function;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.*;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.util.constants.Capabilities;

/** Context (current connection state) of a connection */
public class BaseContext implements Context {

  private final long serverCapabilities;
  private final long clientCapabilities;
  private final byte[] seed;
  private final ServerVersion version;
  private final boolean eofDeprecated;
  private final boolean skipMeta;
  private final Function<ReadableByteBuf, ColumnDecoder> columnDecoderFunction;
  private final Configuration conf;
  private final ExceptionFactory exceptionFactory;
  private final boolean canUseTransactionIsolation;

  /** LRU prepare cache object */
  private final PrepareCache prepareCache;

  private final HostAddress hostAddress;

  /** Server status context */
  protected int serverStatus;

  private Long autoIncrement;

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

  private String redirectUrl = null;

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
    this.serverStatus = handshake.getServerStatus();
    this.version = handshake.getVersion();
    this.clientCapabilities = clientCapabilities;
    this.eofDeprecated = hasClientCapability(Capabilities.CLIENT_DEPRECATE_EOF);
    this.skipMeta = hasClientCapability(Capabilities.CACHE_METADATA);
    this.columnDecoderFunction =
        hasClientCapability(Capabilities.EXTENDED_TYPE_INFO)
            ? ColumnDecoder::decode
            : ColumnDecoder::decodeStd;
    this.conf = conf;
    this.database = conf.database();
    this.exceptionFactory = exceptionFactory;
    this.prepareCache = prepareCache;
    this.canUseTransactionIsolation =
        (version.isMariaDBServer()
                && version.getMajorVersion() < 23
                && version.versionGreaterOrEqual(11, 1, 1))
            || (!version.isMariaDBServer()
                && ((version.getMajorVersion() >= 8 && version.versionGreaterOrEqual(8, 0, 3))
                    || (version.getMajorVersion() < 8 && version.versionGreaterOrEqual(5, 7, 20))));
  }

  public long getThreadId() {
    return threadId;
  }

  public void setThreadId(long connectionId) {
    threadId = connectionId;
  }

  public byte[] getSeed() {
    return seed;
  }

  public boolean hasServerCapability(long flag) {
    return (serverCapabilities & flag) > 0;
  }

  public boolean hasClientCapability(long flag) {
    return (clientCapabilities & flag) > 0;
  }

  public boolean permitPipeline() {
    return !conf.disablePipeline() && (clientCapabilities & STMT_BULK_OPERATIONS) > 0;
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

  public Function<ReadableByteBuf, ColumnDecoder> getColumnDecoderFunction() {
    return columnDecoderFunction;
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

  public Integer getTransactionIsolationLevel() {
    return transactionIsolationLevel;
  }

  public void setTransactionIsolationLevel(Integer transactionIsolationLevel) {
    this.transactionIsolationLevel = transactionIsolationLevel;
  }

  public PrepareCache getPrepareCache() {
    return prepareCache;
  }

  public void resetPrepareCache() {
    if (prepareCache != null) prepareCache.reset();
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

  public void setTreadsConnected(long threadsConnected) {
    if (hostAddress != null) hostAddress.setThreadsConnected(threadsConnected);
  }

  @Override
  public Long getAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(long autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public String getRedirectUrl() {
    return redirectUrl;
  }

  public boolean canUseTransactionIsolation() {
    return canUseTransactionIsolation;
  }

  @Override
  public void setRedirectUrl(String redirectUrl) {
    this.redirectUrl = redirectUrl;
  }
}
