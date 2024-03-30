// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.ConnectionEvent;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.impl.StandardClient;
import org.mariadb.jdbc.client.util.ClosableLock;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.message.client.ChangeDbPacket;
import org.mariadb.jdbc.message.client.PingPacket;
import org.mariadb.jdbc.message.client.QueryPacket;
import org.mariadb.jdbc.message.client.ResetPacket;
import org.mariadb.jdbc.util.NativeSql;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.CatalogTerm;
import org.mariadb.jdbc.util.constants.ConnectionState;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.timeout.NoOpQueryTimeoutHandler;
import org.mariadb.jdbc.util.timeout.QueryTimeoutHandler;
import org.mariadb.jdbc.util.timeout.QueryTimeoutHandlerImpl;

/** Public Connection class */
public class Connection implements java.sql.Connection {

  // Redundant character escape '\\}' in RegExp is expected for Android. see CONJ-1161
  private static final Pattern CALLABLE_STATEMENT_PATTERN =
      Pattern.compile(
          "^(\\s*\\{)?\\s*((\\?\\s*=)?(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*"
              + "call(\\s*/\\*([^*]|\\*[^/])*\\*/)*\\s*((((`[^`]+`)|([^`\\}]+))\\.)?"
              + "((`[^`]+`)|([^`\\}(]+)))\\s*(\\(.*\\))?(\\s*/\\*([^*]|\\*[^/])*\\*/)*"
              + "\\s*(#.*)?)\\s*(\\}\\s*)?$",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final ClosableLock lock;
  private final Configuration conf;
  private final Client client;
  private final Properties clientInfo = new Properties();
  private final AtomicInteger savepointId = new AtomicInteger();
  private final boolean canUseServerTimeout;
  private final boolean canCachePrepStmts;
  private final boolean canUseServerMaxRows;
  private final int defaultFetchSize;
  private final boolean forceTransactionEnd;
  private ExceptionFactory exceptionFactory;
  private int lowercaseTableNames = -1;
  private boolean readOnly;
  private MariaDbPoolConnection poolConnection;
  private QueryTimeoutHandler queryTimeoutHandler;

  /**
   * Connection construction.
   *
   * @param conf configuration
   * @param lock thread safe locker
   * @param client client object
   */
  @SuppressWarnings({"this-escape"})
  public Connection(Configuration conf, ClosableLock lock, Client client) {
    this.conf = conf;
    this.forceTransactionEnd =
        Boolean.parseBoolean(conf.nonMappedOptions().getProperty("forceTransactionEnd", "false"));
    this.lock = lock;
    this.exceptionFactory = client.getExceptionFactory().setConnection(this);
    this.client = client;
    Context context = this.client.getContext();
    this.canUseServerTimeout =
        context.getVersion().isMariaDBServer()
            && context.getVersion().versionGreaterOrEqual(10, 1, 2);
    this.queryTimeoutHandler =
        this.canUseServerTimeout
            ? NoOpQueryTimeoutHandler.INSTANCE
            : new QueryTimeoutHandlerImpl(this, lock);
    this.canUseServerMaxRows =
        context.getVersion().isMariaDBServer()
            && context.getVersion().versionGreaterOrEqual(10, 3, 0);
    this.canCachePrepStmts = context.getConf().cachePrepStmts();
    this.defaultFetchSize = context.getConf().defaultFetchSize();
  }

  /**
   * Internal method. Indicate that connection is created from internal pool
   *
   * @param poolConnection PoolConnection
   */
  public void setPoolConnection(MariaDbPoolConnection poolConnection) {
    this.poolConnection = poolConnection;
    this.exceptionFactory = exceptionFactory.setPoolConnection(poolConnection);
  }

  /**
   * Cancels the current query - clones the current protocol and executes a query using the new
   * connection.
   *
   * @throws SQLException never thrown
   */
  public void cancelCurrentQuery() throws SQLException {
    // prefer relying on IP compare to DNS if not using Unix socket/PIPE
    String currentIp = client.getSocketIp();
    HostAddress hostAddress =
        currentIp == null
            ? client.getHostAddress()
            : HostAddress.from(
                currentIp, client.getHostAddress().port, client.getHostAddress().primary);

    try (Client cli = new StandardClient(conf, hostAddress, new ClosableLock(), true)) {
      cli.execute(new QueryPacket("KILL QUERY " + client.getContext().getThreadId()), false);
    }
  }

  @Override
  public Statement createStatement() {
    return new Statement(
        this,
        lock,
        canUseServerTimeout,
        canUseServerMaxRows,
        Statement.RETURN_GENERATED_KEYS,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        defaultFetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return prepareInternal(
        sql,
        Statement.NO_GENERATED_KEYS,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        conf.useServerPrepStmts());
  }

  /**
   * Prepare statement creation
   *
   * @param sql sql
   * @param autoGeneratedKeys auto generated key required
   * @param resultSetType result-set type
   * @param resultSetConcurrency concurrency
   * @param useBinary use server prepare statement
   * @return prepared statement
   * @throws SQLException if Prepare fails
   */
  public PreparedStatement prepareInternal(
      String sql,
      int autoGeneratedKeys,
      int resultSetType,
      int resultSetConcurrency,
      boolean useBinary)
      throws SQLException {
    checkNotClosed();
    if (useBinary && !sql.startsWith("/*client prepare*/")) {
      try {
        return new ServerPreparedStatement(
            NativeSql.parse(sql, client.getContext()),
            this,
            lock,
            canUseServerTimeout,
            canUseServerMaxRows,
            canCachePrepStmts,
            autoGeneratedKeys,
            resultSetType,
            resultSetConcurrency,
            defaultFetchSize);
      } catch (SQLException e) {
        // failover to client
      }
    }
    return new ClientPreparedStatement(
        NativeSql.parse(sql, client.getContext()),
        this,
        lock,
        canUseServerTimeout,
        canUseServerMaxRows,
        autoGeneratedKeys,
        resultSetType,
        resultSetConcurrency,
        defaultFetchSize);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return NativeSql.parse(sql, client.getContext());
  }

  @Override
  public boolean getAutoCommit() {
    return (client.getContext().getServerStatus() & ServerStatus.AUTOCOMMIT) > 0;
  }

  @Override
  @SuppressWarnings("try")
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit == getAutoCommit()) {
      return;
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      getContext().addStateFlag(ConnectionState.STATE_AUTOCOMMIT);
      client.execute(
          new QueryPacket(((autoCommit) ? "set autocommit=1" : "set autocommit=0")), true);
    }
  }

  @Override
  @SuppressWarnings("try")
  public void commit() throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      if (forceTransactionEnd
          || (client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        client.execute(new QueryPacket("COMMIT"), false);
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void rollback() throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      if (forceTransactionEnd
          || (client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        client.execute(new QueryPacket("ROLLBACK"), true);
      }
    }
  }

  @Override
  public void close() throws SQLException {
    if (poolConnection != null) {
      poolConnection.fireConnectionClosed(new ConnectionEvent(poolConnection));
      return;
    }
    client.close();
  }

  @Override
  public boolean isClosed() {
    return client.isClosed();
  }

  /**
   * Connection context.
   *
   * @return connection context.
   */
  public Context getContext() {
    return client.getContext();
  }

  /**
   * Are table case-sensitive or not . Default Value: 0 (Unix), 1 (Windows), 2 (Mac OS X). If set to
   * 0 (the default on Unix-based systems), table names and aliases and database names are compared
   * in a case-sensitive manner. If set to 1 (the default on Windows), names are stored in lowercase
   * and not compared in a case-sensitive manner. If set to 2 (the default on Mac OS X), names are
   * stored as declared, but compared in lowercase.
   *
   * @return int value.
   * @throws SQLException if a connection error occur
   */
  public int getLowercaseTableNames() throws SQLException {
    if (lowercaseTableNames == -1) {
      try (java.sql.Statement st = createStatement()) {
        try (ResultSet rs = st.executeQuery("select @@lower_case_table_names")) {
          rs.next();
          lowercaseTableNames = rs.getInt(1);
        }
      }
    }
    return lowercaseTableNames;
  }

  @Override
  public DatabaseMetaData getMetaData() {
    return new DatabaseMetaData(this, this.conf);
  }

  @Override
  public boolean isReadOnly() {
    return this.readOnly;
  }

  @Override
  @SuppressWarnings("try")
  public void setReadOnly(boolean readOnly) throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      if (this.readOnly != readOnly) {
        client.setReadOnly(readOnly);
      }
      this.readOnly = readOnly;
      getContext().addStateFlag(ConnectionState.STATE_READ_ONLY);
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    if (conf.useCatalogTerm() == CatalogTerm.UseCatalog) return getDatabase();
    return "def";
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    if (conf.useCatalogTerm() == CatalogTerm.UseCatalog) setDatabase(catalog);
  }

  @Override
  public String getSchema() throws SQLException {
    if (conf.useCatalogTerm() == CatalogTerm.UseSchema) return getDatabase();
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    if (conf.useCatalogTerm() == CatalogTerm.UseSchema) setDatabase(schema);
  }

  private String getDatabase() throws SQLException {
    if (client.getContext().hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)) {
      return client.getContext().getDatabase();
    }

    try (Statement stmt = createStatement()) {
      ResultSet rs = stmt.executeQuery("select database()");
      rs.next();
      client.getContext().setDatabase(rs.getString(1));
      return client.getContext().getDatabase();
    }
  }

  @SuppressWarnings("try")
  private void setDatabase(String database) throws SQLException {
    // null catalog means keep current.
    // there is no possibility to set no database when one is selected
    if (database == null
        || (client.getContext().hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)
            && database.equals(client.getContext().getDatabase()))) {
      return;
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      getContext().addStateFlag(ConnectionState.STATE_DATABASE);
      client.execute(new ChangeDbPacket(database), true);
      client.getContext().setDatabase(database);
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    boolean useContextState =
        conf.useLocalSessionState()
            || (client.getContext().hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)
                && ((client.getContext().getVersion().isMariaDBServer()
                        && (client.getContext().getVersion().versionGreaterOrEqual(10, 2, 2)))
                    || client.getContext().getVersion().versionGreaterOrEqual(5, 7, 0)));
    if (useContextState && client.getContext().getTransactionIsolationLevel() != null) {
      return client.getContext().getTransactionIsolationLevel();
    }

    String sql =
        client.getContext().canUseTransactionIsolation()
            ? "SELECT @@session.transaction_isolation"
            : "SELECT @@session.tx_isolation";

    try (Statement stmt = createStatement()) {
      ResultSet rs = stmt.executeQuery(sql);
      if (rs.next()) {
        final String response = rs.getString(1);
        switch (response) {
          case "REPEATABLE-READ":
            return java.sql.Connection.TRANSACTION_REPEATABLE_READ;

          case "READ-UNCOMMITTED":
            return java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;

          case "READ-COMMITTED":
            return java.sql.Connection.TRANSACTION_READ_COMMITTED;

          case "SERIALIZABLE":
            return java.sql.Connection.TRANSACTION_SERIALIZABLE;

          default:
            throw exceptionFactory.create(
                String.format(
                    "Could not get transaction isolation level: Invalid value \"%s\"", response));
        }
      }
      throw exceptionFactory.create("Failed to retrieve transaction isolation");
    }
  }

  @Override
  @SuppressWarnings("try")
  public void setTransactionIsolation(int level) throws SQLException {
    boolean useContextState =
        conf.useLocalSessionState()
            || (client.getContext().hasClientCapability(Capabilities.CLIENT_SESSION_TRACK)
                && ((client.getContext().getVersion().isMariaDBServer()
                        && (client.getContext().getVersion().versionGreaterOrEqual(10, 2, 2)))
                    || client.getContext().getVersion().versionGreaterOrEqual(5, 7, 0)));

    if (useContextState
        && client.getContext().getTransactionIsolationLevel() != null
        && level == client.getContext().getTransactionIsolationLevel()) {
      return;
    }

    String query = "SET SESSION TRANSACTION ISOLATION LEVEL";
    switch (level) {
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
      default:
        throw new SQLException("Unsupported transaction isolation level");
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      checkNotClosed();
      getContext().addStateFlag(ConnectionState.STATE_TRANSACTION_ISOLATION);
      if (conf.useLocalSessionState()) client.getContext().setTransactionIsolationLevel(level);
      client.execute(new QueryPacket(query), true);
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkNotClosed();
    if (client.getContext().getWarning() == 0) {
      return null;
    }

    SQLWarning last = null;
    SQLWarning first = null;

    try (Statement st = this.createStatement()) {
      try (ResultSet rs = st.executeQuery("show warnings")) {
        // returned result set has 'level', 'code' and 'message' columns, in this order.
        while (rs.next()) {
          int code = rs.getInt(2);
          String message = rs.getString(3);
          SQLWarning warning = new SQLWarning(message, null, code);
          if (first == null) {
            first = warning;
          } else {
            last.setNextWarning(warning);
          }
          last = warning;
        }
      }
    }
    return first;
  }

  @Override
  public void clearWarnings() {
    client.getContext().setWarning(0);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkNotClosed();
    return new Statement(
        this,
        lock,
        canUseServerTimeout,
        canUseServerMaxRows,
        Statement.RETURN_GENERATED_KEYS,
        resultSetType,
        resultSetConcurrency,
        defaultFetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return prepareInternal(
        sql,
        Statement.RETURN_GENERATED_KEYS,
        resultSetType,
        resultSetConcurrency,
        conf.useServerPrepStmts());
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkNotClosed();
    Matcher matcher = CALLABLE_STATEMENT_PATTERN.matcher(sql);
    if (!matcher.matches()) {
      throw new SQLSyntaxErrorException(
          "invalid callable syntax. must be like {[?=]call <procedure/function name>[(?,?, ...)]}\n"
              + " but was : "
              + sql);
    }

    String query = NativeSql.parse(matcher.group(2), client.getContext());

    boolean isFunction = (matcher.group(3) != null);
    String databaseAndProcedure = matcher.group(8);
    String database = matcher.group(10);
    String procedureName = matcher.group(13);
    String arguments = matcher.group(16);
    if (database == null) {
      database = getCatalog();
    }

    if (isFunction) {
      return new FunctionStatement(
          this,
          database,
          databaseAndProcedure,
          (arguments == null) ? "()" : arguments,
          lock,
          canUseServerTimeout,
          canUseServerMaxRows,
          canCachePrepStmts,
          resultSetType,
          resultSetConcurrency);
    } else {
      return new ProcedureStatement(
          this,
          query,
          database,
          procedureName,
          lock,
          canUseServerTimeout,
          canUseServerMaxRows,
          canCachePrepStmts,
          resultSetType,
          resultSetConcurrency);
    }
  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    return new HashMap<>();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw exceptionFactory.notSupported("TypeMap are not supported");
  }

  @Override
  public int getHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public void setHoldability(int holdability) {
    // not supported
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    MariaDbSavepoint savepoint = new MariaDbSavepoint(savepointId.incrementAndGet());
    client.execute(new QueryPacket("SAVEPOINT `" + savepoint.rawValue() + "`"), true);
    return savepoint;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    MariaDbSavepoint savepoint = new MariaDbSavepoint(name.replace("`", "``"));
    client.execute(new QueryPacket("SAVEPOINT `" + savepoint.rawValue() + "`"), true);
    return savepoint;
  }

  @Override
  @SuppressWarnings("try")
  public void rollback(java.sql.Savepoint savepoint) throws SQLException {
    checkNotClosed();
    try (ClosableLock ignore = lock.closeableLock()) {
      if ((client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        if (savepoint instanceof Connection.MariaDbSavepoint) {
          client.execute(
              new QueryPacket(
                  "ROLLBACK TO SAVEPOINT `"
                      + ((Connection.MariaDbSavepoint) savepoint).rawValue()
                      + "`"),
              true);
        } else {
          throw exceptionFactory.create("Unknown savepoint type");
        }
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException {
    checkNotClosed();
    try (ClosableLock ignore = lock.closeableLock()) {
      if ((client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
        if (savepoint instanceof Connection.MariaDbSavepoint) {
          client.execute(
              new QueryPacket(
                  "RELEASE SAVEPOINT `"
                      + ((Connection.MariaDbSavepoint) savepoint).rawValue()
                      + "`"),
              true);
        } else {
          throw exceptionFactory.create("Unknown savepoint type");
        }
      }
    }
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkNotClosed();
    return new Statement(
        this,
        lock,
        canUseServerTimeout,
        canUseServerMaxRows,
        Statement.NO_GENERATED_KEYS,
        resultSetType,
        resultSetConcurrency,
        defaultFetchSize);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return prepareInternal(
        sql,
        autoGeneratedKeys,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        conf.useServerPrepStmts());
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
  }

  @Override
  public Clob createClob() {
    return new MariaDbClob();
  }

  @Override
  public Blob createBlob() {
    return new MariaDbBlob();
  }

  @Override
  public NClob createNClob() {
    return new MariaDbClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw exceptionFactory.notSupported("SQLXML type is not supported");
  }

  private void checkNotClosed() throws SQLException {
    if (client.isClosed()) {
      throw exceptionFactory.create("Connection is closed", "08000", 1220);
    }
  }

  @Override
  @SuppressWarnings("try")
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw exceptionFactory.create("the value supplied for timeout is negative");
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      client.execute(PingPacket.INSTANCE, true);
      return true;
    } catch (SQLException sqle) {
      if (poolConnection != null) {
        MariaDbPoolConnection poolConnection = this.poolConnection;
        poolConnection.fireConnectionErrorOccurred(sqle);
        poolConnection.close();
      }
      return false;
    }
  }

  @Override
  public void setClientInfo(String name, String value) {
    clientInfo.put(name, value);
  }

  @Override
  public String getClientInfo(String name) {
    return (String) clientInfo.get(name);
  }

  @Override
  public Properties getClientInfo() {
    return clientInfo;
  }

  @Override
  public void setClientInfo(Properties properties) {
    clientInfo.putAll(properties);
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw exceptionFactory.notSupported("Array type is not supported");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw exceptionFactory.notSupported("Struct type is not supported");
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    if (poolConnection != null) {
      MariaDbPoolConnection poolConnection = this.poolConnection;
      poolConnection.close();
      return;
    }
    client.abort(executor);
  }

  @Override
  @SuppressWarnings("try")
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    if (this.isClosed()) {
      throw exceptionFactory.create(
          "Connection.setNetworkTimeout cannot be called on a closed connection");
    }
    if (milliseconds < 0) {
      throw exceptionFactory.create(
          "Connection.setNetworkTimeout cannot be called with a negative timeout");
    }
    getContext().addStateFlag(ConnectionState.STATE_NETWORK_TIMEOUT);

    try (ClosableLock ignore = lock.closeableLock()) {
      client.setSocketTimeout(milliseconds);
    }
  }

  @Override
  public int getNetworkTimeout() {
    return client.getSocketTimeout();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException("The receiver is not a wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }

  /**
   * Associate connection client
   *
   * @return connection client
   */
  public Client getClient() {
    return client;
  }

  /**
   * Reset connection set has it was after creating a "fresh" new connection.
   * defaultTransactionIsolation must have been initialized.
   *
   * <p>BUT : - session variable state are reset only if option useResetConnection is set and - if
   * using the option "useServerPrepStmts", PREPARE statement are still prepared
   *
   * @throws SQLException if resetting operation failed
   */
  public void reset() throws SQLException {
    // COM_RESET_CONNECTION exist since mysql 5.7.3 and mariadb 10.2.4
    // but not possible to use it with mysql waiting for https://bugs.mysql.com/bug.php?id=97633
    // correction.
    // and mariadb only since https://jira.mariadb.org/browse/MDEV-18281
    boolean useComReset =
        conf.useResetConnection()
            && getContext().getVersion().isMariaDBServer()
            && (getContext().getVersion().versionGreaterOrEqual(10, 3, 13)
                || (getContext().getVersion().getMajorVersion() == 10
                    && getContext().getVersion().getMinorVersion() == 2
                    && getContext().getVersion().versionGreaterOrEqual(10, 2, 22)));

    if (useComReset) {
      client.execute(ResetPacket.INSTANCE, true);
    }

    // in transaction => rollback
    if (forceTransactionEnd
        || (client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0) {
      client.execute(new QueryPacket("ROLLBACK"), true);
    }

    int stateFlag = getContext().getStateFlag();
    if (stateFlag != 0) {
      try {
        if ((stateFlag & ConnectionState.STATE_NETWORK_TIMEOUT) != 0) {
          setNetworkTimeout(null, conf.socketTimeout());
        }
        if ((stateFlag & ConnectionState.STATE_AUTOCOMMIT) != 0) {
          setAutoCommit(conf.autocommit() == null || conf.autocommit());
        }
        if ((stateFlag & ConnectionState.STATE_DATABASE) != 0) {
          setCatalog(conf.database());
        }
        if ((stateFlag & ConnectionState.STATE_READ_ONLY) != 0) {
          setReadOnly(false); // default to master connection
        }
        if (!useComReset && (stateFlag & ConnectionState.STATE_TRANSACTION_ISOLATION) != 0) {
          setTransactionIsolation(
              conf.transactionIsolation() == null
                  ? java.sql.Connection.TRANSACTION_REPEATABLE_READ
                  : conf.transactionIsolation().getLevel());
        }
      } catch (SQLException sqle) {
        throw exceptionFactory.create("error resetting connection");
      }
    }

    client.reset();

    clearWarnings();
  }

  /**
   * Current server thread id.
   *
   * @return current server thread id
   */
  public long getThreadId() {
    return client.getContext().getThreadId();
  }

  /**
   * Fire event to indicate to StatementEventListeners registered on the connection that a
   * PreparedStatement is closed.
   *
   * @param prep prepare statement closing
   */
  public void fireStatementClosed(PreparedStatement prep) {
    if (poolConnection != null) {
      poolConnection.fireStatementClosed(prep);
    }
  }

  /**
   * Get connection exception factory
   *
   * @return connection exception factory
   */
  protected ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  public QueryTimeoutHandler handleTimeout(int queryTimeout) {
    return queryTimeoutHandler.create(queryTimeout);
  }

  /**
   * for _TEST_ only
   *
   * @return current host
   */
  public String __test_host() {
    return this.client.getHostAddress().toString();
  }

  /** Internal Savepoint implementation */
  class MariaDbSavepoint implements java.sql.Savepoint {

    private final String name;
    private final Integer id;

    public MariaDbSavepoint(final String name) {
      this.name = name;
      this.id = null;
    }

    public MariaDbSavepoint(final int savepointId) {
      this.id = savepointId;
      this.name = null;
    }

    /**
     * Retrieves the generated ID for the savepoint that this <code>Savepoint</code> object
     * represents.
     *
     * @return the numeric ID of this savepoint
     */
    public int getSavepointId() throws SQLException {
      if (id == null) {
        throw exceptionFactory.create("Cannot retrieve savepoint id of a named savepoint");
      }
      return id;
    }

    /**
     * Retrieves the name of the savepoint that this <code>Savepoint</code> object represents.
     *
     * @return the name of this savepoint
     */
    public String getSavepointName() throws SQLException {
      if (id != null) {
        throw exceptionFactory.create("Cannot retrieve savepoint name of an unnamed savepoint");
      }
      return name;
    }

    public String rawValue() {
      if (id != null) {
        return "_jid_" + id;
      }
      return name;
    }
  }
}
