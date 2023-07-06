// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.ServerPreparedStatement;
import com.singlestore.jdbc.client.context.BaseContext;
import com.singlestore.jdbc.client.context.Context;
import com.singlestore.jdbc.client.context.RedoContext;
import com.singlestore.jdbc.client.result.Result;
import com.singlestore.jdbc.client.result.StreamingResult;
import com.singlestore.jdbc.client.socket.*;
import com.singlestore.jdbc.message.client.*;
import com.singlestore.jdbc.message.server.Completion;
import com.singlestore.jdbc.message.server.ErrorPacket;
import com.singlestore.jdbc.message.server.InitialHandshakePacket;
import com.singlestore.jdbc.message.server.PrepareResultPacket;
import com.singlestore.jdbc.plugin.credential.Credential;
import com.singlestore.jdbc.plugin.credential.CredentialPlugin;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin;
import com.singlestore.jdbc.util.MutableInt;
import com.singlestore.jdbc.util.Security;
import com.singlestore.jdbc.util.constants.Capabilities;
import com.singlestore.jdbc.util.constants.ServerStatus;
import com.singlestore.jdbc.util.exceptions.ExceptionFactory;
import com.singlestore.jdbc.util.exceptions.MaxAllowedPacketException;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLPermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.SSLSocket;

public class ClientImpl implements Client, AutoCloseable {

  protected final ExceptionFactory exceptionFactory;
  private Socket socket;
  private final MutableInt sequence = new MutableInt();
  private final MutableInt compressionSequence = new MutableInt();
  private final ReentrantLock lock;
  private final Configuration conf;
  private final HostAddress hostAddress;
  private final boolean disablePipeline;
  protected PacketWriter writer;
  protected Context context;
  private boolean closed = false;
  private PacketReader reader;
  private com.singlestore.jdbc.Statement streamStmt = null;
  private ClientMessage streamMsg = null;
  private int socketTimeout;
  private int waitTimeout;
  protected boolean timeOut;

  private TimerTask getTimerTask() {
    return new TimerTask() {
      @Override
      public void run() {
        Thread cancelThread =
            new Thread() {
              @Override
              public void run() {
                boolean lockStatus = lock.tryLock();

                if (!closed) {
                  closed = true;
                  timeOut = true;
                  if (!lockStatus) {
                    // lock not available : query is running
                    // force end by executing an KILL connection
                    try (ClientImpl cli =
                        new ClientImpl(conf, hostAddress, new ReentrantLock(), true)) {
                      cli.execute(new QueryPacket("KILL " + context.getThreadId()));
                    } catch (SQLException e) {
                      // eat
                    }
                  } else {
                    try {
                      QuitPacket.INSTANCE.encode(writer, context);
                    } catch (IOException e) {
                      // eat
                    }
                  }
                  if (streamStmt != null) {
                    try {
                      streamStmt.abort();
                    } catch (SQLException e) {
                      // eat
                    }
                  }
                  closeSocket();
                }

                if (lockStatus) {
                  lock.unlock();
                }
              }
            };
        cancelThread.start();
      }
    };
  }

  public ClientImpl(
      Configuration conf, HostAddress hostAddress, ReentrantLock lock, boolean skipPostCommands)
      throws SQLException {

    this.conf = conf;
    this.lock = lock;
    this.hostAddress = hostAddress;
    this.exceptionFactory = new ExceptionFactory(conf, hostAddress);
    this.disablePipeline =
        Boolean.parseBoolean(conf.nonMappedOptions().getProperty("disablePipeline", "false"));

    this.socketTimeout = conf.socketTimeout();

    String host = hostAddress != null ? hostAddress.host : null;

    try {
      connect(host, skipPostCommands);
    } catch (SQLInvalidAuthorizationSpecException sqlException) {
      // retry when connecting via browser auth token because token might have
      // expired while we were connecting or the cached token was wrong
      // error 2628 is JWT_TOKEN_EXPIRED
      // error 1045 is ACCESS_DENIED_ERROR
      if (conf.credentialPlugin() != null
              && conf.credentialPlugin().type().contains("BROWSER_SSO")
              && sqlException.getErrorCode() == 1045
          || sqlException.getErrorCode() == 2628) {
        BrowserCredentialPlugin credPlugin = (BrowserCredentialPlugin) conf.credentialPlugin();
        // clear both local cache and keyring to force re-acquiring the token
        Loggers.getLogger(ClientImpl.class)
            .debug("Failed to connect with the JWT, retrying browser auth");
        credPlugin.clearKeyring();
        credPlugin.clearLocalCache();
        this.closed = false;
        connect(host, skipPostCommands);
      } else {
        throw sqlException;
      }
    }
  }

  private void connect(String host, boolean skipPostCommands) throws SQLException {
    this.socket = ConnectionHelper.connectSocket(conf, hostAddress);
    try {
      // **********************************************************************
      // creating socket
      // **********************************************************************
      OutputStream out = socket.getOutputStream();
      InputStream in =
          conf.useReadAheadInput()
              ? new ReadAheadBufferedStream(socket.getInputStream())
              : new BufferedInputStream(socket.getInputStream(), 16384);

      assignStream(out, in, conf, null);

      if (conf.socketTimeout() > 0) setSocketTimeout(conf.socketTimeout());

      // read server handshake
      ReadableByteBuf buf = reader.readPacket(true);
      if (buf.getByte() == -1) {
        ErrorPacket errorPacket = new ErrorPacket(buf, null);
        throw this.exceptionFactory.create(
            errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
      }
      final InitialHandshakePacket handshake = InitialHandshakePacket.decode(buf);

      this.exceptionFactory.setThreadId(handshake.getThreadId());
      long clientCapabilities =
          ConnectionHelper.initializeClientCapabilities(conf, handshake.getCapabilities());

      this.context =
          conf.transactionReplay()
              ? new RedoContext(
                  handshake,
                  clientCapabilities,
                  conf,
                  this.exceptionFactory,
                  new PrepareCache(conf.prepStmtCacheSize(), this))
              : new BaseContext(
                  handshake,
                  clientCapabilities,
                  conf,
                  this.exceptionFactory,
                  new PrepareCache(conf.prepStmtCacheSize(), this));

      this.reader.setServerThreadId(handshake.getThreadId(), hostAddress);
      this.writer.setServerThreadId(handshake.getThreadId(), hostAddress);

      byte exchangeCharset = ConnectionHelper.decideLanguage(handshake);

      // **********************************************************************
      // changing to SSL socket if needed
      // **********************************************************************
      SSLSocket sslSocket =
          ConnectionHelper.sslWrapper(
              hostAddress, socket, clientCapabilities, exchangeCharset, context, writer);

      if (sslSocket != null) {
        out = sslSocket.getOutputStream();
        in =
            conf.useReadAheadInput()
                ? new ReadAheadBufferedStream(sslSocket.getInputStream())
                : new BufferedInputStream(sslSocket.getInputStream(), 16384);
        assignStream(out, in, conf, handshake.getThreadId());
      }

      // **********************************************************************
      // handling authentication
      // **********************************************************************
      String authenticationPluginType = handshake.getAuthenticationPluginType();
      CredentialPlugin credentialPlugin = conf.credentialPlugin();
      if (credentialPlugin != null && credentialPlugin.defaultAuthenticationPluginType() != null) {
        authenticationPluginType = credentialPlugin.defaultAuthenticationPluginType();
      }

      if ("mysql_clear_password".equals(authenticationPluginType) && sslSocket == null) {
        throw new IllegalStateException(
            "Cannot send password in clear text if SSL is not enabled.");
      }

      Credential credential = ConnectionHelper.loadCredential(credentialPlugin, conf, hostAddress);

      new HandshakeResponse(
              credential,
              authenticationPluginType,
              context.getSeed(),
              conf,
              host,
              clientCapabilities,
              exchangeCharset)
          .encode(writer, context);
      writer.flush();

      ConnectionHelper.authenticationHandler(credential, writer, reader, context);

      // **********************************************************************
      // activate compression if required
      // **********************************************************************
      if ((clientCapabilities & Capabilities.COMPRESS) != 0) {
        assignStream(
            new CompressOutputStream(out, compressionSequence),
            new CompressInputStream(in, compressionSequence),
            conf,
            handshake.getThreadId());
      }

      // **********************************************************************
      // post queries
      // **********************************************************************
      if (!skipPostCommands) {
        postConnectionQueries();
      }
    } catch (IOException ioException) {
      destroySocket();

      String errorMsg =
          String.format(
              "Could not connect to %s:%s : %s", host, socket.getPort(), ioException.getMessage());
      if (host == null) {
        errorMsg = String.format("Could not connect to socket : %s", ioException.getMessage());
      }

      throw exceptionFactory.create(errorMsg, "08000", ioException);
    } catch (SQLException sqlException) {
      destroySocket();
      throw sqlException;
    }
  }

  private void assignStream(OutputStream out, InputStream in, Configuration conf, Long threadId) {
    this.writer = new PacketWriter(out, conf.maxQuerySizeToLog(), sequence, compressionSequence);
    this.writer.setServerThreadId(threadId, hostAddress);

    this.reader = new PacketReader(in, conf, sequence);
    this.reader.setServerThreadId(threadId, hostAddress);
  }

  /** Closing socket in case of Connection error after socket creation. */
  protected void destroySocket() {
    closed = true;
    try {
      this.reader.close();
    } catch (IOException ee) {
      // eat exception
    }
    try {
      this.writer.close();
    } catch (IOException ee) {
      // eat exception
    }
    try {
      this.socket.close();
    } catch (IOException ee) {
      // eat exception
    }
  }

  private void postConnectionQueries() throws SQLException {
    List<String> commands = new ArrayList<>();
    int resInd = 0;
    if (conf.sessionVariables() != null) {
      commands.add("set " + Security.parseSessionVariables(conf.sessionVariables()));
      resInd++;
    }
    commands.add("SELECT @@max_allowed_packet, @@wait_timeout");

    try {
      List<Completion> res;
      ClientMessage[] msgs = new ClientMessage[commands.size()];
      for (int i = 0; i < commands.size(); i++) {
        msgs[i] = new QueryPacket(commands.get(i));
      }
      res =
          executePipeline(
              msgs, null, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);

      // read max allowed packet
      Result result = (Result) res.get(resInd);
      result.next();

      waitTimeout = Integer.parseInt(result.getString(2));
      writer.setMaxAllowedPacket(Integer.parseInt(result.getString(1)));

    } catch (SQLException sqlException) {
      throw exceptionFactory.create("Initialization command fail", "08000", sqlException);
    }
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
  }

  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
      Logger logger = Loggers.getLogger(ClientImpl.class);
      if (logger.isDebugEnabled() && message.description() != null) {
        logger.debug("execute query: {}", message.description());
      }
      return message.encode(writer, context);
    } catch (IOException ioException) {
      if (ioException instanceof MaxAllowedPacketException) {
        if (((MaxAllowedPacketException) ioException).isMustReconnect()) {
          destroySocket();
          throw exceptionFactory
              .withSql(message.description())
              .create(
                  "Packet too big for current server max_allowed_packet value",
                  "08000",
                  ioException);
        }
        throw exceptionFactory
            .withSql(message.description())
            .create(
                "Packet too big for current server max_allowed_packet value", "HZ000", ioException);
      }
      destroySocket();
      if (timeOut) {
        throw exceptionFactory
            .withSql(message.description())
            .create("Socket error: query timed out", "08000", ioException);
      } else {
        throw exceptionFactory
            .withSql(message.description())
            .create("Socket error", "08000", ioException);
      }
    }
  }

  public List<Completion> execute(ClientMessage message) throws SQLException {
    return execute(
        message, null, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  public List<Completion> execute(ClientMessage message, com.singlestore.jdbc.Statement stmt)
      throws SQLException {
    return execute(
        message, stmt, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  public List<Completion> executePipeline(
      ClientMessage[] messages,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    List<Completion> results = new ArrayList<>();

    int readCounter = 0;
    int[] responseMsg = new int[messages.length];
    try {
      if (disablePipeline) {
        for (readCounter = 0; readCounter < messages.length; readCounter++) {
          results.addAll(
              execute(
                  messages[readCounter],
                  stmt,
                  fetchSize,
                  maxRows,
                  resultSetConcurrency,
                  resultSetType,
                  closeOnCompletion));
        }
      } else {
        for (int i = 0; i < messages.length; i++) {
          responseMsg[i] = sendQuery(messages[i]);
        }
        while (readCounter < messages.length) {
          readCounter++;
          for (int j = 0; j < responseMsg[readCounter - 1]; j++) {
            results.addAll(
                readResponse(
                    stmt,
                    messages[readCounter - 1],
                    fetchSize,
                    maxRows,
                    resultSetConcurrency,
                    resultSetType,
                    closeOnCompletion));
          }
        }
      }
      return results;
    } catch (SQLException sqlException) {

      // read remaining results
      for (int i = readCounter; i < messages.length; i++) {
        for (int j = 0; j < responseMsg[i]; j++) {
          try {
            results.addAll(
                readResponse(
                    stmt,
                    messages[i],
                    fetchSize,
                    maxRows,
                    resultSetConcurrency,
                    resultSetType,
                    closeOnCompletion));
          } catch (SQLException e) {
            // eat
          }
        }
      }

      // prepare associated to PrepareStatement need to be uncached
      for (Completion result : results) {
        if (result instanceof PrepareResultPacket && stmt instanceof ServerPreparedStatement) {
          try {
            ((PrepareResultPacket) result).decrementUse(this, (ServerPreparedStatement) stmt);
          } catch (SQLException e) {
            // eat
          }
        }
      }

      int batchUpdateLength = 0;
      for (ClientMessage message : messages) {
        batchUpdateLength += message.batchUpdateLength();
      }
      throw exceptionFactory.createBatchUpdate(
          results, batchUpdateLength, responseMsg, sqlException);
    }
  }

  public List<Completion> execute(
      ClientMessage message,
      com.singlestore.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {

    if (stmt != null && stmt.getQueryTimeout() > 0) {
      Timer cancelTimer = new Timer();
      try {
        cancelTimer.schedule(getTimerTask(), stmt.getQueryTimeout() * 1000);
        sendQuery(message);
        return readResponse(
            stmt,
            message,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion);
      } finally {
        cancelTimer.cancel();
      }
    } else {
      sendQuery(message);
      return readResponse(
          stmt,
          message,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    }
  }

  public List<Completion> readResponse(
      com.singlestore.jdbc.Statement stmt,
      ClientMessage message,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    checkNotClosed();
    if (streamStmt != null) {
      streamStmt.fetchRemaining();
      streamStmt = null;
    }
    List<Completion> completions = new ArrayList<>();
    readResults(
        stmt,
        message,
        completions,
        fetchSize,
        maxRows,
        resultSetConcurrency,
        resultSetType,
        closeOnCompletion);
    return completions;
  }

  public void readResponse(ClientMessage message) throws SQLException {
    checkNotClosed();
    if (streamStmt != null) {
      streamStmt.fetchRemaining();
      streamStmt = null;
    }
    List<Completion> completions = new ArrayList<>();
    readResults(
        null,
        message,
        completions,
        0,
        0L,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.TYPE_FORWARD_ONLY,
        false);
  }

  public void closePrepare(PrepareResultPacket prepare) throws SQLException {
    checkNotClosed();
    try {
      new ClosePreparePacket(prepare.getStatementId()).encode(writer, context);
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory.create(
          "Socket error during post connection queries: " + ioException.getMessage(),
          "08000",
          ioException);
    }
  }

  public void readStreamingResults(
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    if (streamStmt != null) {
      readResults(
          streamStmt,
          streamMsg,
          completions,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    }
  }

  private void readResults(
      com.singlestore.jdbc.Statement stmt,
      ClientMessage message,
      List<Completion> completions,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    completions.add(
        readPacket(
            stmt,
            message,
            fetchSize,
            maxRows,
            resultSetConcurrency,
            resultSetType,
            closeOnCompletion));

    while ((context.getServerStatus() & ServerStatus.MORE_RESULTS_EXISTS) > 0) {
      completions.add(
          readPacket(
              stmt,
              message,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion));
    }
  }

  public Completion readPacket(ClientMessage message) throws SQLException {
    return readPacket(
        null, message, 0, 0L, ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY, false);
  }

  /**
   * Read server response packet.
   *
   * @see <a href="https://mariadb.com/kb/en/mariadb/4-server-response-packets/">server response
   *     packets</a>
   * @param stmt current statement (null if internal)
   * @param message current message
   * @param fetchSize default fetch size
   * @param resultSetConcurrency concurrency
   * @param resultSetType type
   * @param closeOnCompletion must resultset close statement on completion
   * @throws SQLException if any exception
   */
  public Completion readPacket(
      com.singlestore.jdbc.Statement stmt,
      ClientMessage message,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    try {
      boolean traceEnable = Loggers.getLogger(ClientImpl.class).isTraceEnabled();
      Completion completion =
          message.readPacket(
              stmt,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion,
              reader,
              writer,
              context,
              exceptionFactory,
              lock,
              traceEnable);
      if (completion instanceof StreamingResult && !((StreamingResult) completion).loaded()) {
        streamStmt = stmt;
        streamMsg = message;
      }
      return completion;
    } catch (IOException ioException) {
      destroySocket();
      if (timeOut) {
        throw exceptionFactory
            .withSql(message.description())
            .create("Socket error: query timed out", "08000", ioException);
      } else {
        throw exceptionFactory
            .withSql(message.description())
            .create("Socket error", "08000", ioException);
      }
    }
  }

  protected void checkNotClosed() throws SQLException {
    if (closed) {
      if (timeOut) {
        throw exceptionFactory.create("Connection is closed due to query timed out", "08000", 1220);
      } else {
        throw exceptionFactory.create("Connection is closed", "08000", 1220);
      }
    }
  }

  private void closeSocket() {
    try {
      try {
        long maxCurrentMillis = System.currentTimeMillis() + 10;
        socket.shutdownOutput();
        socket.setSoTimeout(3);
        InputStream is = socket.getInputStream();
        //noinspection StatementWithEmptyBody
        while (is.read() != -1 && System.currentTimeMillis() < maxCurrentMillis) {
          // read byte
        }
      } catch (Throwable t) {
        // eat exception
      }
      writer.close();
      reader.close();
    } catch (IOException e) {
      // eat
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        // socket closed, if any error, so not throwing error
      }
    }
  }

  @Override
  public int getWaitTimeout() {
    return waitTimeout;
  }

  public boolean isClosed() {
    return closed;
  }

  public Context getContext() {
    return context;
  }

  public void abort(Executor executor) throws SQLException {

    SQLPermission sqlPermission = new SQLPermission("callAbort");
    SecurityManager securityManager = System.getSecurityManager();
    if (securityManager != null) {
      securityManager.checkPermission(sqlPermission);
    }
    if (executor == null) {
      throw exceptionFactory.create("Cannot abort the connection: null executor passed");
    }

    //    fireConnectionClosed(new ConnectionEvent(this));
    boolean lockStatus = lock.tryLock();

    if (!this.closed) {
      this.closed = true;

      if (!lockStatus) {
        // lock not available : query is running
        // force end by executing an KILL connection
        try (ClientImpl cli = new ClientImpl(conf, hostAddress, new ReentrantLock(), true)) {
          cli.execute(new QueryPacket("KILL " + context.getThreadId()));
        } catch (SQLException e) {
          // eat
        }
      } else {
        try {
          QuitPacket.INSTANCE.encode(writer, context);
        } catch (IOException e) {
          // eat
        }
      }
      if (streamStmt != null) {
        streamStmt.abort();
      }
      closeSocket();
    }

    if (lockStatus) {
      lock.unlock();
    }
  }

  public int getSocketTimeout() {
    return this.socketTimeout;
  }

  public void setSocketTimeout(int milliseconds) throws SQLException {
    try {
      socketTimeout = milliseconds;
      socket.setSoTimeout(milliseconds);
    } catch (SocketException se) {
      throw exceptionFactory.create("Cannot set the network timeout", "42000", se);
    }
  }

  public void close() throws SQLException {
    boolean locked = lock.tryLock();

    if (!this.closed) {
      this.closed = true;
      try {
        QuitPacket.INSTANCE.encode(writer, context);
      } catch (IOException e) {
        // eat
      }
      closeSocket();
    }

    if (locked) {
      lock.unlock();
    }
  }

  public boolean isPrimary() {
    return hostAddress.primary;
  }

  public ExceptionFactory getExceptionFactory() {
    return exceptionFactory;
  }

  public HostAddress getHostAddress() {
    return hostAddress;
  }

  public void reset() {
    context.resetStateFlag();
    context.getPrepareCache().reset();
  }
}
