// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.pool;

import java.lang.management.ManagementFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/** MariaDB Pool */
public class Pool implements AutoCloseable, PoolMBean {

  private static final Logger logger = Loggers.getLogger(Pool.class);

  private static final int POOL_STATE_OK = 0;
  private static final int POOL_STATE_CLOSING = 1;

  private final AtomicInteger poolState = new AtomicInteger();

  private final Configuration conf;
  private final AtomicInteger pendingRequestNumber = new AtomicInteger();
  private final AtomicInteger totalConnection = new AtomicInteger();

  private final LinkedBlockingDeque<MariaDbInnerPoolConnection> idleConnections;
  private final ThreadPoolExecutor connectionAppender;
  private final BlockingQueue<Runnable> connectionAppenderQueue;

  private final String poolTag;
  private final ScheduledThreadPoolExecutor poolExecutor;
  private final ScheduledFuture<?> scheduledFuture;

  private int waitTimeout;

  /**
   * Create pool from configuration.
   *
   * @param conf configuration parser
   * @param poolIndex pool index to permit distinction of thread name
   * @param poolExecutor pools common executor
   */
  @SuppressWarnings({"this-escape"})
  public Pool(Configuration conf, int poolIndex, ScheduledThreadPoolExecutor poolExecutor) {

    this.conf = conf;
    poolTag = generatePoolTag(poolIndex);

    // one thread to add new connection to pool.
    connectionAppenderQueue = new ArrayBlockingQueue<>(conf.maxPoolSize());
    connectionAppender =
        new ThreadPoolExecutor(
            1,
            1,
            10,
            TimeUnit.SECONDS,
            connectionAppenderQueue,
            new PoolThreadFactory(poolTag + "-appender"));
    connectionAppender.allowCoreThreadTimeOut(true);
    // create workers, since driver only interact with queue after that (i.e. not using .execute() )
    connectionAppender.prestartCoreThread();

    idleConnections = new LinkedBlockingDeque<>();
    int minDelay =
        Integer.parseInt(conf.nonMappedOptions().getProperty("testMinRemovalDelay", "30"));
    int scheduleDelay = Math.min(minDelay, conf.maxIdleTime() / 2);
    this.poolExecutor = poolExecutor;
    scheduledFuture =
        poolExecutor.scheduleAtFixedRate(
            this::removeIdleTimeoutConnection, scheduleDelay, scheduleDelay, TimeUnit.SECONDS);

    if (conf.registerJmxPool()) {
      try {
        registerJmx();
      } catch (Exception ex) {
        logger.error("pool " + poolTag + " not registered due to exception : " + ex.getMessage());
      }
    }

    // create minimal connection in pool
    try {
      for (int i = 0; i < Math.max(1, conf.minPoolSize()); i++) {
        addConnection();
      }
      waitTimeout = 28800;
      if (!idleConnections.isEmpty()) {
        try (Statement stmt = idleConnections.getFirst().getConnection().createStatement()) {
          ResultSet rs = stmt.executeQuery("SELECT @@wait_timeout");
          if (rs.next()) waitTimeout = rs.getInt(1);
        }
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
    if (totalConnection.get() < conf.maxPoolSize() && poolState.get() == POOL_STATE_OK) {

      // ensure to have one worker if was timeout
      connectionAppender.prestartCoreThread();
      connectionAppenderQueue.offer(
          () -> {
            if ((totalConnection.get() < conf.minPoolSize() || pendingRequestNumber.get() > 0)
                && totalConnection.get() < conf.maxPoolSize()) {
              try {
                addConnection();
              } catch (SQLException sqle) {
                logger.error("error adding connection to pool", sqle);
              }
            }
          });
    }
  }

  /**
   * Removing idle connection. Close them and recreate connection to reach minimal number of
   * connection.
   */
  private void removeIdleTimeoutConnection() {

    // descending iterator since first from queue are the first to be used
    Iterator<MariaDbInnerPoolConnection> iterator = idleConnections.descendingIterator();

    MariaDbInnerPoolConnection item;

    while (iterator.hasNext()) {
      item = iterator.next();

      long idleTime = System.nanoTime() - item.getLastUsed().get();
      boolean timedOut = idleTime > TimeUnit.SECONDS.toNanos(conf.maxIdleTime());

      boolean shouldBeReleased = false;
      Connection con = item.getConnection();
      if (waitTimeout > 0) {

        // idle time is reaching server @@wait_timeout
        if (idleTime > TimeUnit.SECONDS.toNanos(waitTimeout - 45)) {
          shouldBeReleased = true;
        }

        //  idle has reach option maxIdleTime value and pool has more connections than minPoolSiz
        if (timedOut && totalConnection.get() > conf.minPoolSize()) {
          shouldBeReleased = true;
        }

      } else if (timedOut) {
        shouldBeReleased = true;
      }

      if (shouldBeReleased && idleConnections.remove(item)) {

        totalConnection.decrementAndGet();
        silentCloseConnection(con);
        addConnectionRequest();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "pool {} connection {} removed due to inactivity (total:{}, active:{}, pending:{})",
              poolTag,
              con.getThreadId(),
              totalConnection.get(),
              getActiveConnections(),
              pendingRequestNumber.get());
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

    // create new connection
    Connection connection = Driver.connect(conf);
    MariaDbInnerPoolConnection item = new MariaDbInnerPoolConnection(connection);
    item.addConnectionEventListener(
        new ConnectionEventListener() {

          @Override
          public void connectionClosed(ConnectionEvent event) {
            MariaDbInnerPoolConnection item = (MariaDbInnerPoolConnection) event.getSource();
            if (poolState.get() == POOL_STATE_OK) {
              try {
                if (!idleConnections.contains(item)) {
                  item.getConnection().setPoolConnection(null);
                  item.getConnection().reset();
                  idleConnections.addFirst(item);
                  item.getConnection().setPoolConnection(item);
                }
              } catch (SQLException sqle) {

                // sql exception during reset, removing connection from pool
                totalConnection.decrementAndGet();
                silentCloseConnection(item.getConnection());
                logger.debug(
                    "connection {} removed from pool {} due to error during reset (total:{},"
                        + " active:{}, pending:{})",
                    item.getConnection().getThreadId(),
                    poolTag,
                    totalConnection.get(),
                    getActiveConnections(),
                    pendingRequestNumber.get());
              }
            } else {
              // pool is closed, should then not be rendered to pool, but closed.
              try {
                item.getConnection().close();
              } catch (SQLException sqle) {
                // eat
              }
              totalConnection.decrementAndGet();
            }
          }

          @Override
          public void connectionErrorOccurred(ConnectionEvent event) {

            MariaDbInnerPoolConnection item = ((MariaDbInnerPoolConnection) event.getSource());
            totalConnection.decrementAndGet();
            idleConnections.remove(item);

            // ensure that other connection will be validated before being use
            // since one connection failed, better to assume the other might as well
            idleConnections.forEach(MariaDbInnerPoolConnection::ensureValidation);

            silentCloseConnection(item.getConnection());
            addConnectionRequest();
            logger.debug(
                "connection {} removed from pool {} due to having throw a Connection exception"
                    + " (total:{}, active:{}, pending:{})",
                item.getConnection().getThreadId(),
                poolTag,
                totalConnection.get(),
                getActiveConnections(),
                pendingRequestNumber.get());
          }
        });
    if (poolState.get() == POOL_STATE_OK
        && totalConnection.incrementAndGet() <= conf.maxPoolSize()) {
      idleConnections.addFirst(item);

      if (logger.isDebugEnabled()) {
        logger.debug(
            "pool {} new physical connection {} created (total:{}, active:{}, pending:{})",
            poolTag,
            connection.getThreadId(),
            totalConnection.get(),
            getActiveConnections(),
            pendingRequestNumber.get());
      }
      return;
    }

    silentCloseConnection(connection);
  }

  /**
   * Get an existing idle connection in pool.
   *
   * @return an IDLE connection.
   */
  private MariaDbInnerPoolConnection getIdleConnection(long timeout, TimeUnit timeUnit)
      throws InterruptedException {

    while (true) {
      MariaDbInnerPoolConnection item =
          (timeout == 0)
              ? idleConnections.pollFirst()
              : idleConnections.pollFirst(timeout, timeUnit);

      if (item == null) return null;
      try {
        if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - item.getLastUsed().get())
            > conf.poolValidMinDelay()) {

          // validate connection
          if (item.getConnection().isValid(10)) { // 10 seconds timeout
            item.lastUsedToNow();
            return item;
          }

        } else {

          // connection has been retrieved recently -> skip connection validation
          item.lastUsedToNow();
          return item;
        }

      } catch (SQLException sqle) {
        // eat
      }

      // validation failed
      silentAbortConnection(item.getConnection());
      addConnectionRequest();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "pool {} connection {} removed from pool due to failed validation (total:{},"
                + " active:{}, pending:{})",
            poolTag,
            item.getConnection().getThreadId(),
            totalConnection.get(),
            getActiveConnections(),
            pendingRequestNumber.get());
      }
    }
  }

  private void silentCloseConnection(Connection con) {
    con.setPoolConnection(null);
    try {
      con.close();
    } catch (SQLException ex) {
      // eat exception
    }
  }

  private void silentAbortConnection(Connection con) {
    con.setPoolConnection(null);
    try {
      con.abort(poolExecutor);
    } catch (SQLException ex) {
      // eat exception
    }
  }

  /**
   * Retrieve new connection. If possible return idle connection, if not, stack connection query,
   * ask for a connection creation, and loop until a connection become idle / a new connection is
   * created.
   *
   * @return a connection object
   * @throws SQLException if no connection is created when reaching timeout (connectTimeout option)
   */
  public MariaDbInnerPoolConnection getPoolConnection() throws SQLException {
    pendingRequestNumber.incrementAndGet();
    MariaDbInnerPoolConnection poolConnection;
    try {
      // try to get Idle connection if any (with a very small timeout)
      if ((poolConnection =
              getIdleConnection(totalConnection.get() > 4 ? 0 : 50, TimeUnit.MICROSECONDS))
          != null) {
        return poolConnection;
      }

      // ask for new connection creation if max is not reached
      addConnectionRequest();

      // try to create new connection if semaphore permit it
      if ((poolConnection =
              getIdleConnection(
                  TimeUnit.MILLISECONDS.toNanos(conf.connectTimeout()), TimeUnit.NANOSECONDS))
          != null) {
        return poolConnection;
      }

      throw new SQLException(
          String.format(
              "No connection available within the specified time (option 'connectTimeout': %s ms)",
              NumberFormat.getInstance().format(conf.connectTimeout())));

    } catch (InterruptedException interrupted) {
      throw new SQLException("Thread was interrupted", "70100", interrupted);
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
  public MariaDbInnerPoolConnection getPoolConnection(String username, String password)
      throws SQLException {
    if (username == null
        ? conf.user() == null
        : username.equals(conf.user()) && (password == null || password.isEmpty())
            ? conf.password() == null
            : password.equals(conf.password())) {
      return getPoolConnection();
    }

    Configuration tmpConf = conf.clone(username, password);
    return new MariaDbInnerPoolConnection(Driver.connect(tmpConf));
  }

  private String generatePoolTag(int poolIndex) {
    if (conf.poolName() == null) {
      return "MariaDB-pool";
    }
    return conf.poolName() + "-" + poolIndex;
  }

  /**
   * Get current configuration
   *
   * @return configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /** Close pool and underlying connections. */
  @Override
  public void close() {
    try {
      synchronized (this) {
        Pools.remove(this);
        poolState.set(POOL_STATE_CLOSING);
        pendingRequestNumber.set(0);

        scheduledFuture.cancel(false);
        connectionAppender.shutdown();

        try {
          connectionAppender.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException i) {
          // eat
        }

        if (logger.isInfoEnabled()) {
          logger.debug(
              "closing pool {} (total:{}, active:{}, pending:{})",
              poolTag,
              totalConnection.get(),
              getActiveConnections(),
              pendingRequestNumber.get());
        }

        ExecutorService connectionRemover =
            new ThreadPoolExecutor(
                totalConnection.get(),
                conf.maxPoolSize(),
                10,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(conf.maxPoolSize()),
                new PoolThreadFactory(poolTag + "-destroyer"));

        // loop for up to 10 seconds to close not used connection
        long start = System.nanoTime();
        do {
          closeAll(idleConnections);
          if (totalConnection.get() > 0) {
            Thread.sleep(0, 10_00);
          }
        } while (totalConnection.get() > 0
            && TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 10);

        // after having wait for 10 seconds, force removal, even if used connections
        if (totalConnection.get() > 0 || idleConnections.isEmpty()) {
          closeAll(idleConnections);
        }

        connectionRemover.shutdown();
        try {
          unRegisterJmx();
        } catch (Exception exception) {
          // eat
        }
        connectionRemover.awaitTermination(10, TimeUnit.SECONDS);
      }
    } catch (Exception e) {
      // eat
    }
  }

  private void closeAll(Collection<MariaDbInnerPoolConnection> collection) {
    synchronized (collection) { // synchronized mandatory to iterate Collections.synchronizedList()
      for (MariaDbInnerPoolConnection item : collection) {
        collection.remove(item);
        totalConnection.decrementAndGet();
        silentAbortConnection(item.getConnection());
      }
    }
  }

  /**
   * return pool tag
   *
   * @return pool tag
   */
  public String getPoolTag() {
    return poolTag;
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

    synchronized (mbs) {
      if (!mbs.isRegistered(name)) {
        mbs.registerMBean(this, name);
      }
    }
  }

  private void unRegisterJmx() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    String jmxName = poolTag.replace(":", "_");
    ObjectName name = new ObjectName("org.mariadb.jdbc.pool:type=" + jmxName);

    synchronized (mbs) {
      if (mbs.isRegistered(name)) {
        mbs.unregisterMBean(name);
      }
    }
  }

  /**
   * For testing purpose only.
   *
   * @return current thread id's
   */
  public List<Long> testGetConnectionIdleThreadIds() {
    List<Long> threadIds = new ArrayList<>();
    for (MariaDbInnerPoolConnection pooledConnection : idleConnections) {
      threadIds.add(pooledConnection.getConnection().getThreadId());
    }
    return threadIds;
  }
}
