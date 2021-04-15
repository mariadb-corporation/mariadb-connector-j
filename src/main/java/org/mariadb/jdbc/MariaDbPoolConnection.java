// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.*;

public class MariaDbPoolConnection implements PooledConnection {

  private final Connection connection;
  private final List<ConnectionEventListener> connectionEventListeners;
  private final List<StatementEventListener> statementEventListeners;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public MariaDbPoolConnection(Connection connection) {
    this.connection = connection;
    this.connection.setPoolConnection(this);
    statementEventListeners = new CopyOnWriteArrayList<>();
    connectionEventListeners = new CopyOnWriteArrayList<>();
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    connectionEventListeners.add(listener);
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    connectionEventListeners.remove(listener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    statementEventListeners.add(listener);
  }

  @Override
  public void removeStatementEventListener(StatementEventListener listener) {
    statementEventListeners.remove(listener);
  }

  public void fireStatementClosed(PreparedStatement statement) {
    StatementEvent event = new StatementEvent(this, statement);
    for (StatementEventListener listener : statementEventListeners) {
      listener.statementClosed(event);
    }
  }

  public void fireStatementErrorOccurred(PreparedStatement statement, SQLException returnEx) {
    StatementEvent event = new StatementEvent(this, statement, returnEx);
    for (StatementEventListener listener : statementEventListeners) {
      listener.statementErrorOccurred(event);
    }
  }

  public void fireConnectionClosed(ConnectionEvent event) {
    for (ConnectionEventListener listener : connectionEventListeners) {
      listener.connectionClosed(event);
    }
  }

  public void fireConnectionErrorOccurred(SQLException returnEx) {
    ConnectionEvent event = new ConnectionEvent(this, returnEx);
    for (ConnectionEventListener listener : connectionEventListeners) {
      listener.connectionErrorOccurred(event);
    }
  }

  @Override
  public void close() throws SQLException {
    fireConnectionClosed(new ConnectionEvent(this));
    connection.setPoolConnection(null);
    connection.close();
  }
}
