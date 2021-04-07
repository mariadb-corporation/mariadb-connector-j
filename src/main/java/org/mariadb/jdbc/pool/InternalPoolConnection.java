package org.mariadb.jdbc.pool;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.*;
import org.mariadb.jdbc.Connection;
import org.mariadb.jdbc.MariaDbPoolConnection;

public class InternalPoolConnection extends MariaDbPoolConnection {
  private final AtomicLong lastUsed;

  /**
   * Constructor.
   *
   * @param connection connection to retrieve connection options
   */
  public InternalPoolConnection(Connection connection) {
    super(connection);
    lastUsed = new AtomicLong(System.nanoTime());
  }

  public void close() throws SQLException {
    fireConnectionClosed(new ConnectionEvent(this));
  }

  /**
   * Indicate last time this pool connection has been used.
   *
   * @return current last used time (nano).
   */
  public AtomicLong getLastUsed() {
    return lastUsed;
  }

  /** Set last poolConnection use to now. */
  public void lastUsedToNow() {
    lastUsed.set(System.nanoTime());
  }
}
