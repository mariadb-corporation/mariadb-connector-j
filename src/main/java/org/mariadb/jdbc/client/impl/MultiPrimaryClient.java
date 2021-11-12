// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.impl;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.context.RedoContext;
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
  private static final Logger logger = Loggers.getLogger(MultiPrimaryClient.class);

  protected static final ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();
  protected final long deniedListTimeout;
  protected final Configuration conf;
  protected boolean closed = false;
  protected final ReentrantLock lock;
  protected Client currentClient;

  public MultiPrimaryClient(Configuration conf, ReentrantLock lock) throws SQLException {
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

    Optional<HostAddress> host;
    SQLNonTransientConnectionException lastSqle = null;
    int maxRetries = conf.retriesAllDown();

    while ((host = conf.haMode().getAvailableHost(conf.addresses(), denyList, !readOnly))
        .isPresent()) {
      try {
        return conf.transactionReplay()
            ? new ReplayClient(conf, host.get(), lock, false)
            : new StandardClient(conf, host.get(), lock, false);
      } catch (SQLNonTransientConnectionException sqle) {
        lastSqle = sqle;
        denyList.putIfAbsent(host.get(), System.currentTimeMillis() + deniedListTimeout);
        maxRetries--;
      }
    }

    if (failFast) {
      throw (lastSqle != null)
          ? lastSqle
          : new SQLNonTransientConnectionException("all hosts are blacklisted");
    }

    // All server corresponding to type are in deny list
    // return the one with lower denylist timeout
    // (check that server is in conf, because denyList is shared for all instances)
    if (denyList.entrySet().stream()
        .noneMatch(e -> conf.addresses().contains(e.getKey()) && e.getKey().primary != readOnly))
      throw new SQLNonTransientConnectionException(
          String.format("No %s host defined", readOnly ? "replica" : "primary"));
    while (maxRetries > 0) {
      try {
        host =
            denyList.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .filter(
                    e -> conf.addresses().contains(e.getKey()) && e.getKey().primary != readOnly)
                .findFirst()
                .map(Map.Entry::getKey);
        if (host.isPresent()) {
          Client client =
              conf.transactionReplay()
                  ? new ReplayClient(conf, host.get(), lock, false)
                  : new StandardClient(conf, host.get(), lock, false);
          denyList.remove(host.get());
          return client;
        }
        maxRetries--;
      } catch (SQLNonTransientConnectionException sqle) {
        lastSqle = sqle;
        host.ifPresent(
            hostAddress ->
                denyList.putIfAbsent(hostAddress, System.currentTimeMillis() + deniedListTimeout));
        maxRetries--;
        if (maxRetries > 0) {
          try {
            // wait 250ms before looping through
            Thread.sleep(250);
          } catch (InterruptedException interrupted) {
            // interrupted, continue
          }
        }
      }
    }

    throw lastSqle;
  }

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

  protected void replayIfPossible(Client oldClient) throws SQLException {
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
                  "Driver has reconnect connection after a communications link failure with %s. In progress transaction was lost",
                  oldClient.getHostAddress()),
              "25S03");
        }
      } else {
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

  protected void executeTransactionReplay(Client oldCli) throws SQLException {
    // transaction replay
    RedoContext ctx = (RedoContext) oldCli.getContext();
    if (ctx.getTransactionSaver().isDirty()) {
      ctx.getTransactionSaver().clear();
      throw new SQLTransientConnectionException(
          String.format(
              "Driver has reconnect connection after a communications link failure with %s. In progress transaction was too big to be replayed, and was lost",
              oldCli.getHostAddress()),
          "25S03");
    }
    ((ReplayClient) currentClient).transactionReplay(ctx.getTransactionSaver());
  }

  public void syncNewState(Client oldCli) throws SQLException {
    Context oldCtx = oldCli.getContext();
    currentClient.getExceptionFactory().setConnection(oldCli.getExceptionFactory());
    if ((oldCtx.getStateFlag() & ConnectionState.STATE_AUTOCOMMIT) > 0) {
      if ((oldCtx.getServerStatus() & ServerStatus.AUTOCOMMIT)
          != (currentClient.getContext().getServerStatus() & ServerStatus.AUTOCOMMIT)) {
        currentClient.getContext().addStateFlag(ConnectionState.STATE_AUTOCOMMIT);
        currentClient.execute(
            new QueryPacket(
                "set autocommit="
                    + (((oldCtx.getServerStatus() & ServerStatus.AUTOCOMMIT) > 0) ? "1" : "0")));
      }
    }

    if ((oldCtx.getStateFlag() & ConnectionState.STATE_DATABASE) > 0
        && !Objects.equals(currentClient.getContext().getDatabase(), oldCtx.getDatabase())) {
      currentClient.getContext().addStateFlag(ConnectionState.STATE_DATABASE);
      currentClient.execute(new ChangeDbPacket(oldCtx.getDatabase()));
    }

    if ((oldCtx.getStateFlag() & ConnectionState.STATE_NETWORK_TIMEOUT) > 0) {
      currentClient.setSocketTimeout(oldCli.getSocketTimeout());
    }

    if ((oldCtx.getStateFlag() & ConnectionState.STATE_READ_ONLY) > 0
        && !currentClient.getHostAddress().primary
        && currentClient.getContext().getVersion().versionGreaterOrEqual(5, 6, 5)) {
      currentClient.execute(new QueryPacket("SET SESSION TRANSACTION READ ONLY"));
    }

    if ((oldCtx.getStateFlag() & ConnectionState.STATE_TRANSACTION_ISOLATION) > 0
        && currentClient.getContext().getTransactionIsolationLevel()
            != oldCtx.getTransactionIsolationLevel()) {
      String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
      switch (oldCtx.getTransactionIsolationLevel()) {
        case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
          query += " READ UNCOMMITTED";
          break;
        case java.sql.Connection.TRANSACTION_READ_COMMITTED:
          query += " READ COMMITTED";
          break;
        case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
          query += " REPEATABLE READ";
          break;
        case java.sql.Connection.TRANSACTION_SERIALIZABLE:
          query += " SERIALIZABLE";
          break;
      }
      currentClient
          .getContext()
          .setTransactionIsolationLevel(oldCtx.getTransactionIsolationLevel());
      currentClient.execute(new QueryPacket(query));
    }
  }

  @Override
  public List<Completion> execute(ClientMessage message) throws SQLException {
    return execute(
        message, null, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  @Override
  public List<Completion> execute(ClientMessage message, org.mariadb.jdbc.Statement stmt)
      throws SQLException {
    return execute(
        message, stmt, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  @Override
  public List<Completion> execute(
      ClientMessage message,
      Statement stmt,
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
      return currentClient.execute(
          message,
          stmt,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    } catch (SQLNonTransientConnectionException e) {
      HostAddress hostAddress = currentClient.getHostAddress();
      Client oldClient = reConnect();

      if (message instanceof QueryPacket && ((QueryPacket) message).isCommit()) {
        throw new SQLTransientConnectionException(
            String.format(
                "Driver has reconnect connection after a communications failure with %s during a COMMIT statement",
                hostAddress),
            "25S03");
      }

      replayIfPossible(oldClient);

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
          closeOnCompletion);
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
      boolean closeOnCompletion)
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
          closeOnCompletion);
    } catch (SQLException e) {
      if (e instanceof SQLNonTransientConnectionException
          || (e.getCause() != null && e.getCause() instanceof SQLNonTransientConnectionException)) {
        Client oldClient = reConnect();
        replayIfPossible(oldClient);
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
            closeOnCompletion);
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
    currentClient.close();
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
  public int getWaitTimeout() {
    return currentClient.getWaitTimeout();
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

  public boolean isPrimary() {
    return true;
  }

  @Override
  public void reset() {
    currentClient.getContext().resetStateFlag();
    currentClient.getContext().resetPrepareCache();
  }
}
