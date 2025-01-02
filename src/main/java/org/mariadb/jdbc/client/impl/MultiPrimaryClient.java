// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.context.RedoContext;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.message.client.ChangeDbPacket;
import org.mariadb.jdbc.message.client.QueryPacket;
import org.mariadb.jdbc.message.client.RedoableWithPrepareClientMessage;
import org.mariadb.jdbc.util.constants.ConnectionState;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/**
 * Handling connection failing automatic reconnection transparently when possible for multi-master
 * Topology.
 *
 * <p>remark: would have been better using proxy, but for AOT compilation, avoiding to using not
 * supported proxy class.
 */
public class MultiPrimaryClient implements Client {
  /** temporary blacklisted hosts */
  protected static final ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();

  private static final Logger logger = Loggers.getLogger(MultiPrimaryClient.class);

  /** denied timeout */
  protected final long deniedListTimeout;

  /** configuration */
  protected final Configuration conf;

  /** thread locker */
  protected final ClosableLock lock;

  /** is connections explicitly closed */
  protected boolean closed = false;

  /** current client */
  protected Client currentClient;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param lock thread locker
   * @throws SQLException if fail to connect
   */
  @SuppressWarnings({"this-escape"})
  public MultiPrimaryClient(Configuration conf, ClosableLock lock) throws SQLException {
    this.conf = conf;
    this.lock = lock;
    deniedListTimeout =
        Long.parseLong(conf.nonMappedOptions().getProperty("deniedListTimeout", "60000"));
    currentClient = connectHost(false, false);
  }

  /**
   * Trying connecting server.
   *
   * <p>searching each connecting primary / replica connection not temporary denied until found one.
   * searching in temporary denied host if not succeed, until reaching `retriesAllDown` attempts.
   *
   * @param readOnly must connect a replica / primary
   * @param failFast must try only not denied server
   * @return a valid connection client
   * @throws SQLException if not succeed to create a connection.
   */
  protected Client connectHost(boolean readOnly, boolean failFast) throws SQLException {
    int maxRetries = conf.retriesAllDown();

    // First try to connect to available hosts
    try {
      Client client = tryConnectToAvailableHost(readOnly, maxRetries);
      if (client != null) {
        return client;
      }
    } catch (SQLNonTransientConnectionException | SQLTimeoutException lastException) {
      // Handle fail-fast scenario
      if (failFast) {
        throw lastException;
      }
    }

    if (failFast) {
      throw new SQLNonTransientConnectionException("all hosts are blacklisted");
    }

    // Verify valid host configuration exists
    validateHostConfiguration(readOnly);

    // Try connecting to denied hosts as last resort
    return tryConnectToDeniedHost(readOnly, maxRetries);
  }

  private Client tryConnectToAvailableHost(boolean readOnly, int retriesLeft) throws SQLException {
    SQLException lastException = null;
    while (retriesLeft > 0) {
      Optional<HostAddress> host =
          conf.haMode().getAvailableHost(conf.addresses(), denyList, !readOnly);
      if (!host.isPresent()) {
        break;
      }

      try {
        return createClient(host.get());
      } catch (SQLNonTransientConnectionException | SQLTimeoutException e) {
        lastException = e;
        addToDenyList(host.get());
        retriesLeft--;
      }
    }
    if (lastException != null) throw lastException;
    return null;
  }

  private Client tryConnectToDeniedHost(boolean readOnly, int retriesLeft) throws SQLException {
    SQLNonTransientConnectionException lastException = null;

    while (retriesLeft > 0) {
      Optional<HostAddress> host = findHostWithLowestDenyTimeout(readOnly);
      if (!host.isPresent()) {
        retriesLeft--;
        continue;
      }

      try {
        Client client = createClient(host.get());
        denyList.remove(host.get());
        return client;
      } catch (SQLNonTransientConnectionException e) {
        lastException = e;
        host.ifPresent(this::addToDenyList);
        retriesLeft--;
        if (retriesLeft > 0) {
          sleepBeforeRetry();
        }
      }
    }

    throw (lastException != null)
        ? lastException
        : new SQLNonTransientConnectionException("No host");
  }

  private Optional<HostAddress> findHostWithLowestDenyTimeout(boolean readOnly) {
    return denyList.entrySet().stream()
        .sorted(Map.Entry.comparingByValue())
        .filter(e -> conf.addresses().contains(e.getKey()) && e.getKey().primary != readOnly)
        .findFirst()
        .map(Map.Entry::getKey);
  }

  private void validateHostConfiguration(boolean readOnly)
      throws SQLNonTransientConnectionException {
    boolean hasValidHost =
        denyList.entrySet().stream()
            .anyMatch(e -> conf.addresses().contains(e.getKey()) && e.getKey().primary != readOnly);

    if (!hasValidHost) {
      throw new SQLNonTransientConnectionException(
          String.format("No %s host defined", readOnly ? "replica" : "primary"));
    }
  }

  private Client createClient(HostAddress host) throws SQLException {
    return conf.transactionReplay()
        ? new ReplayClient(conf, host, lock, false)
        : new StandardClient(conf, host, lock, false);
  }

  private void addToDenyList(HostAddress host) {
    denyList.putIfAbsent(host, System.currentTimeMillis() + deniedListTimeout);
  }

  private void sleepBeforeRetry() {
    try {
      Thread.sleep(250);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Connection loop
   *
   * @return client connection
   * @throws SQLException if fail to connect
   */
  protected Client reConnect() throws SQLException {

    denyList.putIfAbsent(
        currentClient.getHostAddress(), System.currentTimeMillis() + deniedListTimeout);
    logger.info("Connection error on {}", currentClient.getHostAddress());
    try {
      Client oldClient = currentClient;
      // remove cached prepare from existing server prepare statement
      oldClient.getContext().resetPrepareCache();

      currentClient = connectHost(false, false);
      syncNewState(oldClient);
      return oldClient;

    } catch (SQLNonTransientConnectionException sqle) {
      currentClient = null;
      closed = true;
      throw sqle;
    }
  }

  /**
   * Execute transaction replay if in transaction and configured for it, throw an exception if not
   *
   * @param oldClient previous client
   * @param canRedo if command can be redo even if not in transaction
   * @throws SQLException if not able to replay
   */
  protected void replayIfPossible(Client oldClient, boolean canRedo) throws SQLException {
    // oldClient is only valued if this occurs on master.
    if (oldClient != null) {
      if ((oldClient.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        if (conf.transactionReplay()) {
          executeTransactionReplay(oldClient);
        } else {
          // transaction is lost, but connection is now up again.
          // changing exception to SQLTransientConnectionException
          throw new SQLTransientConnectionException(
              String.format(
                  "Driver has reconnect connection after a communications link failure with %s. In"
                      + " progress transaction was lost",
                  oldClient.getHostAddress()),
              "25S03");
        }
      } else if (!canRedo) {
        // no transaction, but connection is now up again.
        // changing exception to SQLTransientConnectionException
        throw new SQLTransientConnectionException(
            String.format(
                "Driver has reconnect connection after a communications link failure with %s",
                oldClient.getHostAddress()),
            "25S03");
      }
    }
  }

  /**
   * Execute transaction replay
   *
   * @param oldCli previous client
   * @throws SQLException if not able to replay
   */
  protected void executeTransactionReplay(Client oldCli) throws SQLException {
    // transaction replay
    RedoContext ctx = (RedoContext) oldCli.getContext();
    if (ctx.getTransactionSaver().isDirty()) {
      ctx.getTransactionSaver().clear();
      throw new SQLTransientConnectionException(
          String.format(
              "Driver has reconnect connection after a communications link failure with %s. In"
                  + " progress transaction was too big to be replayed, and was lost",
              oldCli.getHostAddress()),
          "25S03");
    }
    ((ReplayClient) currentClient).transactionReplay(ctx.getTransactionSaver());
  }

  /**
   * Synchronized previous and new client states.
   *
   * @param oldCli previous client
   * @throws SQLException if error occurs
   */
  /**
   * Synchronizes client states between previous and new clients.
   *
   * @param oldCli previous client instance
   * @throws SQLException if synchronization error occurs
   */
  public void syncNewState(Client oldCli) throws SQLException {
    Context oldCtx = oldCli.getContext();
    syncExceptionFactory(oldCli);
    syncAutoCommit(oldCtx);
    syncDatabase(oldCtx);
    syncNetworkTimeout(oldCtx, oldCli);
    syncReadOnlyState(oldCtx);
    syncTransactionIsolation(oldCtx);
  }

  private void syncExceptionFactory(Client oldCli) {
    currentClient.getExceptionFactory().setConnection(oldCli.getExceptionFactory());
  }

  private void syncAutoCommit(Context oldCtx) throws SQLException {
    if (!isAutoCommitSyncRequired(oldCtx)) {
      return;
    }

    currentClient.getContext().addStateFlag(ConnectionState.STATE_AUTOCOMMIT);
    String autoCommitValue = ((oldCtx.getServerStatus() & ServerStatus.AUTOCOMMIT) > 0) ? "1" : "0";
    currentClient.execute(new QueryPacket("set autocommit=" + autoCommitValue), true);
  }

  private boolean isAutoCommitSyncRequired(Context oldCtx) {
    return (oldCtx.getStateFlag() & ConnectionState.STATE_AUTOCOMMIT) > 0
        && (oldCtx.getServerStatus() & ServerStatus.AUTOCOMMIT)
            != (currentClient.getContext().getServerStatus() & ServerStatus.AUTOCOMMIT);
  }

  private void syncDatabase(Context oldCtx) throws SQLException {
    if (!isDatabaseSyncRequired(oldCtx)) {
      return;
    }

    currentClient.getContext().addStateFlag(ConnectionState.STATE_DATABASE);
    if (oldCtx.getDatabase() != null) {
      currentClient.execute(new ChangeDbPacket(oldCtx.getDatabase()), true);
    }
    currentClient.getContext().setDatabase(oldCtx.getDatabase());
  }

  private boolean isDatabaseSyncRequired(Context oldCtx) {
    return (oldCtx.getStateFlag() & ConnectionState.STATE_DATABASE) > 0
        && !Objects.equals(currentClient.getContext().getDatabase(), oldCtx.getDatabase());
  }

  private void syncNetworkTimeout(Context oldCtx, Client oldCli) throws SQLException {
    if ((oldCtx.getStateFlag() & ConnectionState.STATE_NETWORK_TIMEOUT) > 0) {
      currentClient.setSocketTimeout(oldCli.getSocketTimeout());
    }
  }

  private void syncReadOnlyState(Context oldCtx) throws SQLException {
    if (!isReadOnlySyncRequired(oldCtx)) {
      return;
    }
    currentClient.execute(new QueryPacket("SET SESSION TRANSACTION READ ONLY"), true);
  }

  private boolean isReadOnlySyncRequired(Context oldCtx) {
    return (oldCtx.getStateFlag() & ConnectionState.STATE_READ_ONLY) > 0
        && !currentClient.getHostAddress().primary
        && currentClient.getContext().getVersion().versionGreaterOrEqual(5, 6, 5);
  }

  private void syncTransactionIsolation(Context oldCtx) throws SQLException {
    if (!isTransactionIsolationSyncRequired(oldCtx)) {
      return;
    }

    String query = buildTransactionIsolationQuery(oldCtx.getTransactionIsolationLevel());
    currentClient.getContext().setTransactionIsolationLevel(oldCtx.getTransactionIsolationLevel());
    currentClient.execute(new QueryPacket(query), true);
  }

  private boolean isTransactionIsolationSyncRequired(Context oldCtx) {
    return (oldCtx.getStateFlag() & ConnectionState.STATE_TRANSACTION_ISOLATION) > 0
        && !oldCtx
            .getTransactionIsolationLevel()
            .equals(currentClient.getContext().getTransactionIsolationLevel());
  }

  private String buildTransactionIsolationQuery(int isolationLevel) {
    String baseQuery = "SET SESSION TRANSACTION ISOLATION LEVEL";
    switch (isolationLevel) {
      case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
        return baseQuery + " READ UNCOMMITTED";
      case java.sql.Connection.TRANSACTION_READ_COMMITTED:
        return baseQuery + " READ COMMITTED";
      case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
        return baseQuery + " REPEATABLE READ";
      case java.sql.Connection.TRANSACTION_SERIALIZABLE:
        return baseQuery + " SERIALIZABLE";
      default:
        throw new IllegalArgumentException("Unsupported isolation level: " + isolationLevel);
    }
  }

  @Override
  public List<Completion> execute(ClientMessage message, boolean canRedo) throws SQLException {
    return execute(
        message,
        null,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false,
        canRedo);
  }

  @Override
  public List<Completion> execute(
      ClientMessage message, org.mariadb.jdbc.Statement stmt, boolean canRedo) throws SQLException {
    return execute(
        message,
        stmt,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false,
        canRedo);
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {

    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      return currentClient.execute(
          message,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          canRedo);
    } catch (SQLNonTransientConnectionException e) {
      HostAddress hostAddress = currentClient.getHostAddress();
      Client oldClient = reConnect();

      if (message instanceof QueryPacket && ((QueryPacket) message).isCommit()) {
        throw new SQLTransientConnectionException(
            String.format(
                "Driver has reconnect connection after a communications failure with %s during a"
                    + " COMMIT statement",
                hostAddress),
            "25S03");
      }

      replayIfPossible(oldClient, canRedo);

      if (message instanceof RedoableWithPrepareClientMessage) {
        ((RedoableWithPrepareClientMessage) message).rePrepare(currentClient);
      }
      return currentClient.execute(
          message,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          canRedo);
    }
  }

  @Override
  public List<Completion> executePipeline(
      ClientMessage[] messages,
      Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      return currentClient.executePipeline(
          messages,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion,
          canRedo);
    } catch (SQLException e) {
      if (e instanceof SQLNonTransientConnectionException
          || (e.getCause() != null && e.getCause() instanceof SQLNonTransientConnectionException)) {
        Client oldClient = reConnect();
        replayIfPossible(oldClient, canRedo);
        Arrays.stream(messages)
            .filter(RedoableWithPrepareClientMessage.class::isInstance)
            .map(RedoableWithPrepareClientMessage.class::cast)
            .forEach(
                rd -> {
                  try {
                    rd.rePrepare(currentClient);
                  } catch (SQLException sqle) {
                    // eat
                  }
                });
        return currentClient.executePipeline(
            messages,
            stmt,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion,
            canRedo);
      }
      throw e;
    }
  }

  @Override
  public void readStreamingResults(
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      currentClient.readStreamingResults(
          completions, fetchSize, maxRows, resultSetConcurrency, resultSetType, closeOnCompletion);
    } catch (SQLNonTransientConnectionException e) {
      try {
        reConnect();
      } catch (SQLException e2) {
        throw getExceptionFactory()
            .create("Socket error during result streaming", e2.getSQLState(), e2);
      }
      throw getExceptionFactory().create("Socket error during result streaming", "HY000", e);
    }
  }

  @Override
  public void closePrepare(Prepare prepare) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      currentClient.closePrepare(prepare);
    } catch (SQLNonTransientConnectionException e) {
      reConnect();
    }
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
    currentClient.abort(executor);
  }

  @Override
  public void close() throws SQLException {
    closed = true;
    if (currentClient != null) currentClient.close();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
  }

  @Override
  public int getSocketTimeout() {
    return currentClient.getSocketTimeout();
  }

  @Override
  public void setSocketTimeout(int milliseconds) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }

    try {
      currentClient.setSocketTimeout(milliseconds);
    } catch (SQLNonTransientConnectionException e) {
      reConnect();
      currentClient.setSocketTimeout(milliseconds);
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public Context getContext() {
    return currentClient.getContext();
  }

  @Override
  public ExceptionFactory getExceptionFactory() {
    return currentClient.getExceptionFactory();
  }

  @Override
  public HostAddress getHostAddress() {
    return currentClient.getHostAddress();
  }

  @Override
  public String getSocketIp() {
    return currentClient.getSocketIp();
  }

  public boolean isPrimary() {
    return true;
  }

  @Override
  public void reset() {
    currentClient.getContext().resetStateFlag();
    currentClient.getContext().resetPrepareCache();
  }
}
