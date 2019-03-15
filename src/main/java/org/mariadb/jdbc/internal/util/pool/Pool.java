/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

import java.lang.management.ManagementFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbPooledConnection;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.Options;
import org.mariadb.jdbc.internal.util.Utils;
import org.mariadb.jdbc.internal.util.exceptions.ExceptionMapper;
import org.mariadb.jdbc.internal.util.scheduler.MariaDbThreadFactory;

public class Pool implements AutoCloseable, PoolMBean {

  private static final Logger logger = LoggerFactory.getLogger(Pool.class);

  private static final int POOL_STATE_OK = 0;
  private static final int POOL_STATE_CLOSING = 1;

  private final AtomicInteger poolState = new AtomicInteger();

  private final UrlParser urlParser;
  private final Options options;
  private final AtomicInteger pendingRequestNumber = new AtomicInteger();
  private final AtomicInteger totalConnection = new AtomicInteger();

  private final LinkedBlockingDeque<MariaDbPooledConnection> idleConnections;
  private final ThreadPoolExecutor connectionAppender;
  private final BlockingQueue<Runnable> connectionAppenderQueue;

  private final String poolTag;
  private final ScheduledThreadPoolExecutor poolExecutor;
  private final ScheduledFuture scheduledFuture;
  private GlobalStateInfo globalInfo;

  private int maxIdleTime;
  private long timeToConnectNanos;
  private long connectionTime = 0;

  /**
   * Create pool from configuration.
   *
   * @param urlParser    configuration parser
   * @param poolIndex    pool index to permit distinction of thread name
   * @param poolExecutor pools common executor
   */
  public Pool(UrlParser urlParser, int poolIndex, ScheduledThreadPoolExecutor poolExecutor) {

    this.urlParser = urlParser;
    options = urlParser.getOptions();
    this.maxIdleTime = options.maxIdleTime;
    poolTag = generatePoolTag(poolIndex);

    //one thread to add new connection to pool.
    connectionAppenderQueue = new ArrayBlockingQueue<>(options.maxPoolSize);
    connectionAppender = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
        connectionAppenderQueue, new MariaDbThreadFactory(poolTag + "-appender"));
    connectionAppender.allowCoreThreadTimeOut(true);
    //create workers, since driver only interact with queue after that (i.e. not using .execute() )
    connectionAppender.prestartCoreThread();

    idleConnections = new LinkedBlockingDeque<>();

    int scheduleDelay = Math.min(30, maxIdleTime / 2);
    this.poolExecutor = poolExecutor;
    scheduledFuture = poolExecutor
        .scheduleAtFixedRate(this::removeIdleTimeoutConnection, scheduleDelay, scheduleDelay,
            TimeUnit.SECONDS);

    if (options.registerJmxPool) {
      try {
        registerJmx();
      } catch (Exception ex) {
        logger.error("pool " + poolTag + " not registered due to exception : " + ex.getMessage());
      }
    }

    //create minimal connection in pool
    try {
      for (int i = 0; i < options.minPoolSize; i++) {
        addConnection();
      }
    } catch (SQLException sqle) {
      logger.error("error initializing pool connection", sqle);
    }

  }


  /**
   * Add new connection if needed. Only one thread create new connection, so new connection request
   * will wait to newly created connection or for a released connection.
   */
  private void addConnectionRequest() {
    if (totalConnection.get() < options.maxPoolSize && poolState.get() == POOL_STATE_OK) {

      //ensure to have one worker if was timeout
      connectionAppender.prestartCoreThread();
      connectionAppenderQueue.offer(() -> {

        if ((totalConnection.get() < options.minPoolSize || pendingRequestNumber.get() > 0)
            && totalConnection.get() < options.maxPoolSize) {
          try {
            addConnection();
          } catch (SQLException sqle) {
            //eat
          }
        }
      });
    }
  }

  /**
   * Removing idle connection.
   * Close them and recreate connection to reach minimal number of connection.
   */
  private void removeIdleTimeoutConnection() {

    //descending iterator since first from queue are the first to be used
    Iterator<MariaDbPooledConnection> iterator = idleConnections.descendingIterator();

    MariaDbPooledConnection item;

    while (iterator.hasNext()) {
      item = iterator.next();

      long idleTime = System.nanoTime() - item.getLastUsed().get();
      boolean timedOut = idleTime > TimeUnit.SECONDS.toNanos(maxIdleTime);

      boolean shouldBeReleased = false;

      if (globalInfo != null) {

        // idle time is reaching server @@wait_timeout
        if (idleTime > TimeUnit.SECONDS.toNanos(globalInfo.getWaitTimeout() - 45)) {
          shouldBeReleased = true;
        }

        //  idle has reach option maxIdleTime value and pool has more connections than minPoolSiz
        if (timedOut && totalConnection.get() > options.minPoolSize) {
          shouldBeReleased = true;
        }

      } else if (timedOut) {
        shouldBeReleased = true;
      }

      if (shouldBeReleased && idleConnections.remove(item)) {

        totalConnection.decrementAndGet();
        silentCloseConnection(item);
        addConnectionRequest();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "pool {} connection removed due to inactivity (total:{}, active:{}, pending:{})",
              poolTag, totalConnection.get(), getActiveConnections(), pendingRequestNumber.get());
        }

      }
    }

  }

  /**
   * Create new connection.
   *
   * @throws SQLException if connection creation failed
   */
  private void addConnection() throws SQLException {

    //create new connection
    Protocol protocol = Utils.retrieveProxy(urlParser, globalInfo);
    MariaDbConnection connection = new MariaDbConnection(protocol);
    MariaDbPooledConnection pooledConnection = createPoolConnection(connection);

    if (options.staticGlobal) {
      //on first connection load initial state
      if (globalInfo == null) {
        initializePoolGlobalState(connection);
      }
      //set default transaction isolation level to permit resetting to initial state
      connection.setDefaultTransactionIsolation(globalInfo.getDefaultTransactionIsolation());
    } else {
      //set default transaction isolation level to permit resetting to initial state
      connection.setDefaultTransactionIsolation(connection.getTransactionIsolation());
    }

    if (poolState.get() == POOL_STATE_OK
        && totalConnection.incrementAndGet() <= options.maxPoolSize) {
      idleConnections.addFirst(pooledConnection);

      if (logger.isDebugEnabled()) {
        logger.debug("pool {} new physical connection created (total:{}, active:{}, pending:{})",
            poolTag, totalConnection.get(), getActiveConnections(), pendingRequestNumber.get());
      }
      return;
    }

    silentCloseConnection(pooledConnection);
  }

  private MariaDbPooledConnection getIdleConnection() throws InterruptedException {
    return getIdleConnection(0, TimeUnit.NANOSECONDS);
  }

  /**
   * Get an existing idle connection in pool.
   *
   * @return an IDLE connection.
   */
  private MariaDbPooledConnection getIdleConnection(long timeout, TimeUnit timeUnit)
      throws InterruptedException {

    while (true) {
      MariaDbPooledConnection item =
              (timeout == 0) ? idleConnections.pollFirst() : idleConnections.pollFirst(timeout, timeUnit);

      if (item != null) {
        MariaDbConnection connection = item.getConnection();
        try {
          if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - item.getLastUsed().get())
                  > options.poolValidMinDelay) {

            //validate connection
            if (connection.isValid(10)) { //10 seconds timeout
              item.lastUsedToNow();
              return item;
            }

          } else {

            // connection has been retrieved recently -> skip connection validation
            item.lastUsedToNow();
            return item;

          }

        } catch (SQLException sqle) {
          //eat
        }

        totalConnection.decrementAndGet();

        // validation failed
        silentAbortConnection(item);
        addConnectionRequest();
        if (logger.isDebugEnabled()) {
          logger.debug(
                  "pool {} connection removed from pool due to failed validation (total:{}, active:{}, pending:{})",
                  poolTag, totalConnection.get(), getActiveConnections(), pendingRequestNumber.get());
        }
        continue;
      }

      return null;

    }
  }

  private void silentCloseConnection(MariaDbPooledConnection item) {
    try {
      item.close();
    } catch (SQLException ex) {
      //eat exception
    }
  }

  private void silentAbortConnection(MariaDbPooledConnection item) {
    try {
      item.abort(poolExecutor);
    } catch (SQLException ex) {
      //eat exception
    }
  }

  private MariaDbPooledConnection createPoolConnection(MariaDbConnection connection) {
    MariaDbPooledConnection pooledConnection = new MariaDbPooledConnection(connection);
    pooledConnection.addConnectionEventListener(new ConnectionEventListener() {

      @Override
      public void connectionClosed(ConnectionEvent event) {
        MariaDbPooledConnection item = (MariaDbPooledConnection) event.getSource();
        if (poolState.get() == POOL_STATE_OK) {
          try {
            if (!idleConnections.contains(item)) {
              item.getConnection().reset();
              idleConnections.addFirst(item);
            }
          } catch (SQLException sqle) {

            //sql exception during reset, removing connection from pool
            totalConnection.decrementAndGet();
            silentCloseConnection(item);
            logger.debug("connection removed from pool {} due to error during reset", poolTag);

          }
        } else {
          //pool is closed, should then not be render to pool, but closed.
          try {
            item.close();
          } catch (SQLException sqle) {
            //eat
          }
          totalConnection.decrementAndGet();
        }
      }

      @Override
      public void connectionErrorOccurred(ConnectionEvent event) {

        MariaDbPooledConnection item = ((MariaDbPooledConnection) event.getSource());
        if (idleConnections.remove(item)) {
          totalConnection.decrementAndGet();
        }
        silentCloseConnection(item);
        addConnectionRequest();
        logger.debug("connection {} removed from pool {} due to having throw a Connection exception (total:{}, active:{}, pending:{})",
                item.getConnection().getServerThreadId(), poolTag, totalConnection.get(), getActiveConnections(), pendingRequestNumber.get());

      }

    });
    return pooledConnection;
  }

  /**
   * Retrieve new connection. If possible return idle connection, if not, stack connection query,
   * ask for a connection creation, and loop until a connection become idle / a new connection is
   * created.
   *
   * @return a connection object
   * @throws SQLException if no connection is created when reaching timeout (connectTimeout option)
   **/
  public MariaDbConnection getConnection() throws SQLException {

    pendingRequestNumber.incrementAndGet();

    MariaDbPooledConnection pooledConnection;

    try {

      //try to get Idle connection if any (with a very small timeout)
      if ((pooledConnection = getIdleConnection(totalConnection.get() > 4 ? 0 : 50,
          TimeUnit.MICROSECONDS)) != null) {
        return pooledConnection.getConnection();
      }

      // ask for new connection creation if max is not reached
      addConnectionRequest();

      //try to create new connection if semaphore permit it
      if ((pooledConnection = getIdleConnection(
          TimeUnit.MILLISECONDS.toNanos(options.connectTimeout),
          TimeUnit.NANOSECONDS)) != null) {
        return pooledConnection.getConnection();
      }

      throw ExceptionMapper.connException("No connection available within the specified time "
          + "(option 'connectTimeout': " + NumberFormat.getInstance().format(options.connectTimeout)
          + " ms)");

    } catch (InterruptedException interrupted) {
      throw ExceptionMapper.connException("Thread was interrupted", interrupted);
    } finally {
      pendingRequestNumber.decrementAndGet();
    }

  }

  /**
   * Get new connection from pool if user and password correspond to pool. If username and password
   * are different from pool, will return a dedicated connection.
   *
   * @param username username
   * @param password password
   * @return connection
   * @throws SQLException if any error occur during connection
   */
  public MariaDbConnection getConnection(String username, String password) throws SQLException {

    try {

      if ((urlParser.getUsername() != null ? urlParser.getUsername().equals(username)
          : username == null)
          && (urlParser.getPassword() != null ? urlParser.getPassword().equals(password)
          : password == null)) {
        return getConnection();
      }

      UrlParser tmpUrlParser = (UrlParser) urlParser.clone();
      tmpUrlParser.setUsername(username);
      tmpUrlParser.setPassword(password);
      Protocol protocol = Utils.retrieveProxy(tmpUrlParser, globalInfo);
      return new MariaDbConnection(protocol);

    } catch (CloneNotSupportedException cloneException) {
      //cannot occur
      throw new SQLException("Error getting connection, parameters cannot be cloned",
          cloneException);
    }
  }

  private String generatePoolTag(int poolIndex) {
    if (options.poolName == null) {
      options.poolName = "MariaDB-pool";
    }
    return options.poolName + "-" + poolIndex;
  }

  public UrlParser getUrlParser() {
    return urlParser;
  }

  /**
   * Close pool and underlying connections.
   *
   * @throws InterruptedException if interrupted
   */
  public void close() throws InterruptedException {
    synchronized (this) {
      Pools.remove(this);
      poolState.set(POOL_STATE_CLOSING);
      pendingRequestNumber.set(0);

      scheduledFuture.cancel(false);
      connectionAppender.shutdown();

      try {
        connectionAppender.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException i) {
        //eat
      }

      if (logger.isInfoEnabled()) {
        logger.info("closing pool {} (total:{}, active:{}, pending:{})",
            poolTag, totalConnection.get(), getActiveConnections(), pendingRequestNumber.get());
      }

      ExecutorService connectionRemover = new ThreadPoolExecutor(totalConnection.get(),
          options.maxPoolSize, 10, TimeUnit.SECONDS,
          new LinkedBlockingQueue<>(options.maxPoolSize),
          new MariaDbThreadFactory(poolTag + "-destroyer"));

      //loop for up to 10 seconds to close not used connection
      long start = System.nanoTime();
      do {
        closeAll(connectionRemover, idleConnections);
        if (totalConnection.get() > 0) {
          Thread.sleep(0, 10_00);
        }
      } while (totalConnection.get() > 0
          && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 10);

      //after having wait for 10 seconds, force removal, even if used connections
      if (totalConnection.get() > 0 || idleConnections.isEmpty()) {
        closeAll(connectionRemover, idleConnections);
      }

      connectionRemover.shutdown();
      try {
        unRegisterJmx();
      } catch (Exception exception) {
        //eat
      }
      connectionRemover.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private void closeAll(ExecutorService connectionRemover,
      Collection<MariaDbPooledConnection> collection) {
    synchronized (collection) { //synchronized mandatory to iterate Collections.synchronizedList()
      for (MariaDbPooledConnection item : collection) {
        collection.remove(item);
        totalConnection.decrementAndGet();
        try {
          item.abort(connectionRemover);
        } catch (SQLException ex) {
          //eat exception
        }
      }
    }
  }

  private void initializePoolGlobalState(MariaDbConnection connection) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      String sql = "SELECT @@max_allowed_packet,"
          + "@@wait_timeout,"
          + "@@autocommit,"
          + "@@auto_increment_increment,"
          + "@@time_zone,"
          + "@@system_time_zone,"
          + "@@tx_isolation";
      if (!connection.isServerMariaDb()) {
        int major = connection.getMetaData().getDatabaseMajorVersion();
        if ((major >= 8 && connection.versionGreaterOrEqual(8, 0, 3))
            || (major < 8 && connection.versionGreaterOrEqual(5, 7, 20))) {
          sql = "SELECT @@max_allowed_packet,"
              + "@@wait_timeout,"
              + "@@autocommit,"
              + "@@auto_increment_increment,"
              + "@@time_zone,"
              + "@@system_time_zone,"
              + "@@transaction_isolation";
        }
      }

      try (ResultSet rs = stmt.executeQuery(sql)) {

        rs.next();

        int transactionIsolation = Utils.transactionFromString(rs.getString(7)); // tx_isolation

        globalInfo = new GlobalStateInfo(
            rs.getLong(1),      // max_allowed_packet
            rs.getInt(2),       // wait_timeout
            rs.getBoolean(3),   // autocommit
            rs.getInt(4),       // autoIncrementIncrement
            rs.getString(5),    // time_zone
            rs.getString(6),    // system_time_zone
            transactionIsolation);

        //ensure that the options "maxIdleTime" is not > to server wait_timeout
        //removing 45s since scheduler check  status every 30s
        maxIdleTime = Math.min(options.maxIdleTime, globalInfo.getWaitTimeout() - 45);
      }
    }
  }

  public String getPoolTag() {
    return poolTag;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    Pool pool = (Pool) obj;

    return poolTag.equals(pool.poolTag);
  }

  @Override
  public int hashCode() {
    return poolTag.hashCode();
  }

  public GlobalStateInfo getGlobalInfo() {
    return globalInfo;
  }

  @Override
  public long getActiveConnections() {
    return totalConnection.get() - idleConnections.size();
  }

  @Override
  public long getTotalConnections() {
    return totalConnection.get();
  }

  @Override
  public long getIdleConnections() {
    return idleConnections.size();
  }

  public long getConnectionRequests() {
    return pendingRequestNumber.get();
  }


  private void registerJmx() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    String jmxName = poolTag.replace(":", "_");
    ObjectName name = new ObjectName("org.mariadb.jdbc.pool:type=" + jmxName);

    if (!mbs.isRegistered(name)) {
      mbs.registerMBean(this, name);
    }
  }

  private void unRegisterJmx() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    String jmxName = poolTag.replace(":", "_");
    ObjectName name = new ObjectName("org.mariadb.jdbc.pool:type=" + jmxName);

    if (mbs.isRegistered(name)) {
      mbs.unregisterMBean(name);
    }
  }

  /**
   * For testing purpose only.
   *
   * @return current thread id's
   */
  public List<Long> testGetConnectionIdleThreadIds() {
    List<Long> threadIds = new ArrayList<>();
    for (MariaDbPooledConnection pooledConnection : idleConnections) {
      threadIds.add(pooledConnection.getConnection().getServerThreadId());
    }
    return threadIds;
  }

  /**
   * JMX method to remove state (will be reinitialized on next connection creation).
   */
  public void resetStaticGlobal() {
    globalInfo = null;
  }

}
