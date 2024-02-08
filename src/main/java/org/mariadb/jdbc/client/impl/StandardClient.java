// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.net.ssl.SSLSocket;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.ServerPreparedStatement;
import org.mariadb.jdbc.client.Client;
import org.mariadb.jdbc.client.Completion;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.BaseContext;
import org.mariadb.jdbc.client.context.RedoContext;
import org.mariadb.jdbc.client.result.Result;
import org.mariadb.jdbc.client.result.StreamingResult;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.socket.impl.*;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.MaxAllowedPacketException;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.message.client.*;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.message.server.PrepareResultPacket;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;
import org.mariadb.jdbc.util.Security;
import org.mariadb.jdbc.util.constants.Capabilities;
import org.mariadb.jdbc.util.constants.ServerStatus;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;

/** Connection client */
public class StandardClient implements Client, AutoCloseable {
  private static final Logger logger = Loggers.getLogger(StandardClient.class);

  /** connection exception factory */
  protected final ExceptionFactory exceptionFactory;

  private final Socket socket;
  private final MutableByte sequence = new MutableByte();
  private final MutableByte compressionSequence = new MutableByte();
  private final ReentrantLock lock;
  private final Configuration conf;
  private final HostAddress hostAddress;
  private final boolean disablePipeline;

  /** connection context */
  protected Context context;

  /** packet writer */
  protected Writer writer;

  private boolean closed = false;
  private Reader reader;
  private org.mariadb.jdbc.Statement streamStmt = null;
  private ClientMessage streamMsg = null;
  private int socketTimeout;

  /**
   * Constructor
   *
   * @param conf configuration
   * @param hostAddress host
   * @param lock thread locker
   * @param skipPostCommands must connection post command be skipped
   * @throws SQLException if connection fails
   */
  @SuppressWarnings({"this-escape"})
  public StandardClient(
      Configuration conf, HostAddress hostAddress, ReentrantLock lock, boolean skipPostCommands)
      throws SQLException {

    this.conf = conf;
    this.lock = lock;
    this.hostAddress = hostAddress;
    this.exceptionFactory = new ExceptionFactory(conf, hostAddress);
    this.disablePipeline = conf.disablePipeline();

    String host = hostAddress != null ? hostAddress.host : null;
    this.socketTimeout = conf.socketTimeout();
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

      if (conf.connectTimeout() > 0) {
        setSocketTimeout(conf.connectTimeout());
      } else if (conf.socketTimeout() > 0) setSocketTimeout(conf.socketTimeout());

      // read server handshake
      ReadableByteBuf buf = reader.readReusablePacket(logger.isTraceEnabled());
      if (buf.getByte() == -1) {
        ErrorPacket errorPacket = new ErrorPacket(buf, null);
        throw this.exceptionFactory.create(
            errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());
      }
      final InitialHandshakePacket handshake = InitialHandshakePacket.decode(buf);

      this.exceptionFactory.setThreadId(handshake.getThreadId());
      long clientCapabilities =
          ConnectionHelper.initializeClientCapabilities(
              conf, handshake.getCapabilities(), hostAddress);
      this.context =
          conf.transactionReplay()
              ? new RedoContext(
                  hostAddress,
                  handshake,
                  clientCapabilities,
                  conf,
                  this.exceptionFactory,
                  new PrepareCache(conf.prepStmtCacheSize(), this))
              : new BaseContext(
                  hostAddress,
                  handshake,
                  clientCapabilities,
                  conf,
                  this.exceptionFactory,
                  conf.cachePrepStmts() ? new PrepareCache(conf.prepStmtCacheSize(), this) : null);

      this.reader.setServerThreadId(handshake.getThreadId(), hostAddress);
      this.writer.setServerThreadId(handshake.getThreadId(), hostAddress);

      // **********************************************************************
      // changing to SSL socket if needed
      // **********************************************************************
      SSLSocket sslSocket =
          ConnectionHelper.sslWrapper(
              hostAddress,
              socket,
              clientCapabilities,
              (byte) handshake.getDefaultCollation(),
              context,
              writer);

      if (sslSocket != null) {
        out = new BufferedOutputStream(sslSocket.getOutputStream(), 16384);
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
      Credential credential = ConnectionHelper.loadCredential(credentialPlugin, conf, hostAddress);

      new HandshakeResponse(
              credential,
              authenticationPluginType,
              context.getSeed(),
              conf,
              host,
              clientCapabilities,
              (byte) handshake.getDefaultCollation())
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
      setSocketTimeout(conf.socketTimeout());

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
    this.writer =
        new PacketWriter(
            out, conf.maxQuerySizeToLog(), conf.maxAllowedPacket(), sequence, compressionSequence);
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

  /**
   * load server timezone and ensure this corresponds to client timezone
   *
   * @throws SQLException if any socket error.
   */
  private String handleTimezone() throws SQLException {
    if (!"disable".equalsIgnoreCase(conf.timezone())) {
      String timeZone = null;
      try {
        Result res =
            (Result)
                execute(new QueryPacket("SELECT @@time_zone, @@system_time_zone"), true).get(0);
        res.next();
        timeZone = res.getString(1);
        if ("SYSTEM".equals(timeZone)) {
          timeZone = res.getString(2);
        }
      } catch (SQLException sqle) {
        Result res =
            (Result)
                execute(
                        new QueryPacket(
                            "SHOW VARIABLES WHERE Variable_name in ("
                                + "'system_time_zone',"
                                + "'time_zone')"),
                        true)
                    .get(0);
        String systemTimeZone = null;
        while (res.next()) {
          if ("system_time_zone".equals(res.getString(1))) {
            systemTimeZone = res.getString(2);
          } else {
            timeZone = res.getString(2);
          }
        }
        if ("SYSTEM".equals(timeZone)) {
          timeZone = systemTimeZone;
        }
      }
      return timeZone;
    }
    return null;
  }

  private void postConnectionQueries() throws SQLException {
    List<String> commands = new ArrayList<>();

    List<String> galeraAllowedStates =
        conf.galeraAllowedState() == null
            ? Collections.emptyList()
            : Arrays.asList(conf.galeraAllowedState().split(","));

    if (hostAddress != null
        && Boolean.TRUE.equals(hostAddress.primary)
        && !galeraAllowedStates.isEmpty()) {
      commands.add("show status like 'wsrep_local_state'");
    }

    String serverTz = conf.timezone() != null ? handleTimezone() : null;
    String sessionVariableQuery = createSessionVariableQuery(serverTz, context);
    if (sessionVariableQuery != null) commands.add(sessionVariableQuery);

    if (hostAddress != null
        && !hostAddress.primary
        && context.getVersion().versionGreaterOrEqual(5, 6, 5)) {
      commands.add("SET SESSION TRANSACTION READ ONLY");
    }

    if (conf.database() != null
        && conf.createDatabaseIfNotExist()
        && (hostAddress == null || hostAddress.primary)) {
      String escapedDb = conf.database().replace("`", "``");
      commands.add(String.format("CREATE DATABASE IF NOT EXISTS `%s`", escapedDb));
      commands.add(String.format("USE `%s`", escapedDb));
    }
    if (context.getCharset() == null || !"utf8mb4".equals(context.getCharset()))
      commands.add("SET NAMES utf8mb4");

    if (conf.initSql() != null) {
      commands.add(conf.initSql());
    }

    if (conf.nonMappedOptions().containsKey("initSql")) {
      String[] initialCommands = conf.nonMappedOptions().get("initSql").toString().split(";");
      Collections.addAll(commands, initialCommands);
    }

    if (!commands.isEmpty()) {
      try {
        List<Completion> res;
        ClientMessage[] msgs = new ClientMessage[commands.size()];
        for (int i = 0; i < commands.size(); i++) {
          msgs[i] = new QueryPacket(commands.get(i));
        }
        res =
            executePipeline(
                msgs,
                null,
                0,
                0L,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.TYPE_FORWARD_ONLY,
                false,
                true);

        if (hostAddress != null
            && Boolean.TRUE.equals(hostAddress.primary)
            && !galeraAllowedStates.isEmpty()) {
          ResultSet rs = (ResultSet) res.get(0);
          if (rs.next()) {
            if (!galeraAllowedStates.contains(rs.getString(2))) {
              throw exceptionFactory.create(
                  String.format("fail to validate Galera state (State is %s)", rs.getString(2)));
            }
          } else {
            throw exceptionFactory.create(
                "fail to validate Galera state (unknown 'wsrep_local_state' state)");
          }
          res.remove(0);
        }

      } catch (SQLException sqlException) {

        if (conf.timezone() != null && !"disable".equalsIgnoreCase(conf.timezone())) {
          // timezone is not valid
          throw exceptionFactory.create(
              String.format(
                  "Setting configured timezone '%s' fail on server.\n"
                      + "Look at https://mariadb.com/kb/en/mysql_tzinfo_to_sql/ to load tz data on"
                      + " server, or set timezone=disable to disable setting client timezone.",
                  conf.timezone()),
              "HY000",
              sqlException);
        }
        throw exceptionFactory.create("Initialization command fail", "08000", sqlException);
      }

      if (conf.returnMultiValuesGeneratedIds()) {
        ClientMessage query = new QueryPacket("SELECT @@auto_increment_increment");
        List<Completion> res = execute(query, true);
        ResultSet rs = (ResultSet) res.get(0);
        if (rs.next()) {
          context.setAutoIncrement(rs.getLong(1));
        }
      }
    }
  }

  /**
   * Create session variable if configuration requires additional commands.
   *
   * @param serverTz server timezone
   * @param context context
   * @return sql setting session command
   */
  public String createSessionVariableQuery(String serverTz, Context context) {
    List<String> sessionCommands = new ArrayList<>();

    // In JDBC, connection must start in autocommit mode
    boolean canRelyOnConnectionFlag =
        context.getVersion().isMariaDBServer()
            && (context.getVersion().versionFixedMajorMinorGreaterOrEqual(10, 4, 33)
                || context.getVersion().versionFixedMajorMinorGreaterOrEqual(10, 5, 24)
                || context.getVersion().versionFixedMajorMinorGreaterOrEqual(10, 6, 17)
                || context.getVersion().versionFixedMajorMinorGreaterOrEqual(10, 11, 7)
                || context.getVersion().versionFixedMajorMinorGreaterOrEqual(11, 0, 5)
                || context.getVersion().versionFixedMajorMinorGreaterOrEqual(11, 1, 4)
                || context.getVersion().versionFixedMajorMinorGreaterOrEqual(11, 2, 3));
    if ((conf.autocommit() == null && (context.getServerStatus() & ServerStatus.AUTOCOMMIT) == 0)
        || (conf.autocommit() != null && !canRelyOnConnectionFlag)
        || (conf.autocommit() != null
            && canRelyOnConnectionFlag
            && ((context.getServerStatus() & ServerStatus.AUTOCOMMIT) > 0) != conf.autocommit())) {
      sessionCommands.add(
          "autocommit=" + ((conf.autocommit() == null || conf.autocommit()) ? "1" : "0"));
    }

    if (conf.returnMultiValuesGeneratedIds()
        && ((context.getVersion().isMariaDBServer()
                && (context.getVersion().versionGreaterOrEqual(10, 2, 2)))
            || context.getVersion().versionGreaterOrEqual(5, 7, 0))) {
      sessionCommands.add(
          "session_track_system_variables ="
              + " CONCAT(@@global.session_track_system_variables,',auto_increment_increment')");
    }

    // add configured session variable if configured
    if (conf.sessionVariables() != null) {
      sessionCommands.add(Security.parseSessionVariables(conf.sessionVariables()));
    }

    // force client timezone to connection to ensure result of now(), ...
    if (conf.timezone() != null && !"disable".equalsIgnoreCase(conf.timezone())) {
      boolean mustSetTimezone = true;
      TimeZone connectionTz =
          "auto".equalsIgnoreCase(conf.timezone())
              ? TimeZone.getDefault()
              : TimeZone.getTimeZone(ZoneId.of(conf.timezone()).normalized());
      ZoneId clientZoneId = connectionTz.toZoneId();

      // try to avoid timezone consideration if server use the same one
      try {
        ZoneId serverZoneId = ZoneId.of(serverTz);
        if (serverZoneId.normalized().equals(clientZoneId)
            || ZoneId.of(serverTz, ZoneId.SHORT_IDS).equals(clientZoneId)) {
          mustSetTimezone = false;
        }
      } catch (DateTimeException e) {
        // eat
      }

      if (mustSetTimezone) {
        if (clientZoneId.getRules().isFixedOffset()) {
          ZoneOffset zoneOffset = clientZoneId.getRules().getOffset(Instant.now());
          if (zoneOffset.getTotalSeconds() == 0) {
            // specific for UTC timezone, server permitting only SYSTEM/UTC offset or named time
            // zone
            // not 'UTC'/'Z'
            sessionCommands.add("time_zone='+00:00'");
          } else {
            sessionCommands.add("time_zone='" + zoneOffset.getId() + "'");
          }
        } else {
          sessionCommands.add("time_zone='" + clientZoneId.normalized() + "'");
        }
      }
    }

    if (conf.transactionIsolation() != null) {
      int major = context.getVersion().getMajorVersion();
      if (!context.getVersion().isMariaDBServer()
          && ((major >= 8 && context.getVersion().versionGreaterOrEqual(8, 0, 3))
              || (major < 8 && context.getVersion().versionGreaterOrEqual(5, 7, 20)))) {
        sessionCommands.add(
            "@@session.transaction_isolation='" + conf.transactionIsolation().getValue() + "'");
      } else {
        sessionCommands.add(
            "@@session.tx_isolation='" + conf.transactionIsolation().getValue() + "'");
      }
    }
    if (!sessionCommands.isEmpty()) {
      return "set " + sessionCommands.stream().collect(Collectors.joining(","));
    }
    return null;
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    if (closed) {
      throw new SQLNonTransientConnectionException("Connection is closed", "08000", 1220);
    }
  }

  /**
   * Send client message to server
   *
   * @param message client message
   * @return number of command send
   * @throws SQLException if socket error occurs
   */
  public int sendQuery(ClientMessage message) throws SQLException {
    checkNotClosed();
    try {
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
      throw exceptionFactory
          .withSql(message.description())
          .create("Socket error", "08000", ioException);
    }
  }

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

  public List<Completion> executePipeline(
      ClientMessage[] messages,
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    List<Completion> results = new ArrayList<>();
    int perMsgCounter = 0;
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
                  closeOnCompletion,
                  canRedo));
        }
      } else {
        for (int i = 0; i < messages.length; i++) {
          responseMsg[i] = sendQuery(messages[i]);
        }
        while (readCounter < messages.length) {
          readCounter++;
          for (perMsgCounter = 0; perMsgCounter < responseMsg[readCounter - 1]; perMsgCounter++) {
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
      if (!closed) {
        results.add(null);
        // read remaining results
        perMsgCounter++;
        for (; perMsgCounter < responseMsg[readCounter - 1]; perMsgCounter++) {
          try {
            results.addAll(
                readResponse(
                    stmt,
                    messages[readCounter - 1],
                    fetchSize,
                    maxRows,
                    resultSetConcurrency,
                    resultSetType,
                    closeOnCompletion));
          } catch (SQLException e) {
            // eat
          }
        }

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
              results.add(null);
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
      org.mariadb.jdbc.Statement stmt,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion,
      boolean canRedo)
      throws SQLException {
    int nbResp = sendQuery(message);
    if (nbResp == 1) {
      return readResponse(
          stmt,
          message,
          fetchSize,
          maxRows,
          resultSetConcurrency,
          resultSetType,
          closeOnCompletion);
    } else {
      if (streamStmt != null) {
        streamStmt.fetchRemaining();
        streamStmt = null;
      }
      List<Completion> completions = new ArrayList<>();
      try {
        while (nbResp-- > 0) {
          readResults(
              stmt,
              message,
              completions,
              fetchSize,
              maxRows,
              resultSetConcurrency,
              resultSetType,
              closeOnCompletion);
        }
        return completions;
      } catch (SQLException e) {
        while (nbResp-- > 0) {
          try {
            readResults(
                stmt,
                message,
                completions,
                fetchSize,
                maxRows,
                resultSetConcurrency,
                resultSetType,
                closeOnCompletion);
          } catch (SQLException ee) {
            // eat
          }
        }
        throw e;
      }
    }
  }

  /**
   * Read server responses for a client message
   *
   * @param stmt statement that issue the message
   * @param message client message sent
   * @param fetchSize fetch size
   * @param maxRows maximum number of rows
   * @param resultSetConcurrency concurrency
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on resultset completion
   * @return list of result
   * @throws SQLException if any error occurs
   */
  public List<Completion> readResponse(
      org.mariadb.jdbc.Statement stmt,
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

  /**
   * Read server response
   *
   * @param message client message that was sent
   * @throws SQLException if any error occurs
   */
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

  public void closePrepare(Prepare prepare) throws SQLException {
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
      org.mariadb.jdbc.Statement stmt,
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

  /**
   * Read a MySQL packet from socket
   *
   * @param message client message issuing the result
   * @return a mysql result
   * @throws SQLException if any error occurs
   */
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
   * @param maxRows maximum row number
   * @param resultSetConcurrency concurrency
   * @param resultSetType type
   * @param closeOnCompletion must resultset close statement on completion
   * @return Completion
   * @throws SQLException if any exception
   */
  public Completion readPacket(
      org.mariadb.jdbc.Statement stmt,
      ClientMessage message,
      int fetchSize,
      long maxRows,
      int resultSetConcurrency,
      int resultSetType,
      boolean closeOnCompletion)
      throws SQLException {
    try {
      boolean traceEnable = logger.isTraceEnabled();
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
              traceEnable,
              message);
      if (completion instanceof StreamingResult && !((StreamingResult) completion).loaded()) {
        streamStmt = stmt;
        streamMsg = message;
      }
      return completion;
    } catch (IOException ioException) {
      destroySocket();
      throw exceptionFactory
          .withSql(message.description())
          .create(
              ioException instanceof SocketTimeoutException
                  ? "Socket timout error"
                  : "Socket error",
              "08000",
              ioException);
    }
  }

  /**
   * Throw an exception if client is closed
   *
   * @throws SQLException if closed
   */
  protected void checkNotClosed() throws SQLException {
    if (closed) {
      throw exceptionFactory.create("Connection is closed", "08000", 1220);
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

  public boolean isClosed() {
    return closed;
  }

  public Context getContext() {
    return context;
  }

  public void abort(Executor executor) throws SQLException {

    if (executor == null) {
      throw exceptionFactory.create("Cannot abort the connection: null executor passed");
    }

    //    fireConnectionClosed(new ConnectionEvent(this));
    boolean lockStatus = lock.tryLock();

    if (!this.closed) {
      this.closed = true;
      logger.debug("aborting connection {}", context.getThreadId());
      if (!lockStatus) {
        // lock not available : query is running
        // force end by executing an KILL connection
        try (StandardClient cli =
            new StandardClient(conf, hostAddress, new ReentrantLock(), true)) {
          cli.execute(new QueryPacket("KILL " + context.getThreadId()), false);
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

  public void close() {
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

  public String getSocketIp() {
    return this.socket.getInetAddress() == null
        ? null
        : this.socket.getInetAddress().getHostAddress();
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
    context.resetPrepareCache();
  }
}
