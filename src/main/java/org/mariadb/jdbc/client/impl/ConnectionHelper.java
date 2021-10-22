// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Arrays;
import java.util.List;
import javax.net.SocketFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.socket.impl.SocketHandlerFunction;
import org.mariadb.jdbc.client.socket.impl.SocketUtility;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.message.client.SslRequestPacket;
import org.mariadb.jdbc.message.server.AuthSwitchPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.plugin.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;
import org.mariadb.jdbc.plugin.TlsSocketPlugin;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.tls.TlsSocketPluginLoader;
import org.mariadb.jdbc.util.ConfigurableSocketFactory;
import org.mariadb.jdbc.util.constants.Capabilities;

public final class ConnectionHelper {

  private static final SocketHandlerFunction socketHandler;

  static {
    SocketHandlerFunction init;
    try {
      init = SocketUtility.getSocketHandler();
    } catch (Throwable t) {
      init = ConnectionHelper::standardSocket;
    }
    socketHandler = init;
  }

  /**
   * Create socket accordingly to options.
   *
   * @param conf Url options
   * @param hostAddress host ( mandatory but for named pipe / unix socket)
   * @return a nex socket
   * @throws IOException if connection error occur
   * @throws SQLException in case of configuration error
   */
  public static Socket createSocket(Configuration conf, HostAddress hostAddress)
      throws IOException, SQLException {
    return socketHandler.apply(conf, hostAddress);
  }

  /**
   * Use standard socket implementation.
   *
   * @param conf url options
   * @param hostAddress host to connect
   * @return socket
   * @throws IOException in case of error establishing socket.
   * @throws SQLException in case host is null
   */
  public static Socket standardSocket(Configuration conf, HostAddress hostAddress)
      throws IOException, SQLException {
    SocketFactory socketFactory;
    String socketFactoryName = conf.socketFactory();
    if (socketFactoryName != null) {
      if (hostAddress == null) throw new SQLException("hostname must be set to connect socket");
      try {
        @SuppressWarnings("unchecked")
        Class<? extends SocketFactory> socketFactoryClass =
            (Class<? extends SocketFactory>) Class.forName(socketFactoryName);
        Constructor<? extends SocketFactory> constructor = socketFactoryClass.getConstructor();
        socketFactory = constructor.newInstance();
        if (socketFactory instanceof ConfigurableSocketFactory) {
          ((ConfigurableSocketFactory) socketFactory).setConfiguration(conf, hostAddress.host);
        }
        return socketFactory.createSocket();
      } catch (Exception exp) {
        throw new IOException(
            "Socket factory failed to initialized with option \"socketFactory\" set to \""
                + conf.socketFactory()
                + "\"",
            exp);
      }
    }
    socketFactory = SocketFactory.getDefault();
    return socketFactory.createSocket();
  }

  public static Socket connectSocket(final Configuration conf, final HostAddress hostAddress)
      throws SQLException {
    Socket socket;
    try {
      socket = createSocket(conf, hostAddress);
      SocketHelper.setSocketOption(conf, socket);
      if (!socket.isConnected()) {
        InetSocketAddress sockAddr =
            conf.pipe() == null && conf.localSocket() == null
                ? new InetSocketAddress(hostAddress.host, hostAddress.port)
                : null;
        socket.connect(sockAddr, conf.connectTimeout());
      }
      return socket;

    } catch (IOException ioe) {
      throw new SQLNonTransientConnectionException(
          String.format(
              "Socket fail to connect to host:%s. %s",
              hostAddress == null ? conf.localSocket() : hostAddress, ioe.getMessage()),
          "08000",
          ioe);
    }
  }

  public static long initializeClientCapabilities(
      final Configuration configuration, final long serverCapabilities) {
    long capabilities =
        Capabilities.IGNORE_SPACE
            | Capabilities.CLIENT_PROTOCOL_41
            | Capabilities.TRANSACTIONS
            | Capabilities.SECURE_CONNECTION
            | Capabilities.MULTI_RESULTS
            | Capabilities.PS_MULTI_RESULTS
            | Capabilities.PLUGIN_AUTH
            | Capabilities.CONNECT_ATTRS
            | Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA
            | Capabilities.CLIENT_SESSION_TRACK
            | Capabilities.MARIADB_CLIENT_EXTENDED_TYPE_INFO;

    if (Boolean.parseBoolean(configuration.nonMappedOptions().getProperty("enableSkipMeta", "true"))
        && (serverCapabilities & Capabilities.MARIADB_CLIENT_CACHE_METADATA) != 0) {
      capabilities |= Capabilities.MARIADB_CLIENT_CACHE_METADATA;
    }

    if (configuration.useBulkStmts()) {
      capabilities |= Capabilities.MARIADB_CLIENT_STMT_BULK_OPERATIONS;
    }

    if (!configuration.useAffectedRows()) {
      capabilities |= Capabilities.FOUND_ROWS;
    }

    if (configuration.allowMultiQueries()) {
      capabilities |= Capabilities.MULTI_STATEMENTS;
    }

    if (configuration.allowLocalInfile()) {
      capabilities |= Capabilities.LOCAL_FILES;
    }

    // useEof is a technical option
    boolean useEof =
        Boolean.parseBoolean(configuration.nonMappedOptions().getProperty("useEof", "true"));
    if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) != 0 && useEof) {
      capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
    }

    if (configuration.useCompression() && ((serverCapabilities & Capabilities.COMPRESS) != 0)) {
      capabilities |= Capabilities.COMPRESS;
    }

    if (configuration.database() != null) {
      capabilities |= Capabilities.CONNECT_WITH_DB;
    }
    return capabilities;
  }

  /**
   * Default collation used for string exchanges with server. Always return 4 bytes utf8 collation
   * for server that permit it.
   *
   * @param handshake initial handshake packet
   * @return collation byte
   */
  public static byte decideLanguage(InitialHandshakePacket handshake) {
    short serverLanguage = handshake.getDefaultCollation();
    // return current server utf8mb4 collation
    if (serverLanguage == 45 // utf8mb4_general_ci
        || serverLanguage == 46 // utf8mb4_bin
        || (serverLanguage >= 224 && serverLanguage <= 247)) {
      return (byte) serverLanguage;
    }
    if (!handshake.getVersion().versionGreaterOrEqual(5, 5, 0)) {
      // 5.1 version doesn't know 4 bytes utf8
      return (byte) 33; // utf8_general_ci
    }
    return (byte) 224; // UTF8MB4_UNICODE_CI;
  }

  public static void authenticationHandler(
      Credential credential, Writer writer, Reader reader, Context context)
      throws SQLException, IOException {

    writer.permitTrace(true);
    Configuration conf = context.getConf();
    ReadableByteBuf buf = reader.readPacket(false);

    authentication_loop:
    while (true) {
      switch (buf.getByte() & 0xFF) {
        case 0xFE:
          // *************************************************************************************
          // Authentication Switch Request see
          // https://mariadb.com/kb/en/library/connection/#authentication-switch-request
          // *************************************************************************************
          AuthSwitchPacket authSwitchPacket = AuthSwitchPacket.decode(buf);
          AuthenticationPlugin authenticationPlugin =
              AuthenticationPluginLoader.get(authSwitchPacket.getPlugin(), conf);

          authenticationPlugin.initialize(
              credential.getPassword(), authSwitchPacket.getSeed(), conf);
          buf = authenticationPlugin.process(writer, reader, context);
          break;

        case 0xFF:
          // *************************************************************************************
          // ERR_Packet
          // see https://mariadb.com/kb/en/library/err_packet/
          // *************************************************************************************
          ErrorPacket errorPacket = new ErrorPacket(buf, context);
          throw context
              .getExceptionFactory()
              .create(
                  errorPacket.getMessage(), errorPacket.getSqlState(), errorPacket.getErrorCode());

        case 0x00:
          // *************************************************************************************
          // OK_Packet -> Authenticated !
          // see https://mariadb.com/kb/en/library/ok_packet/
          // *************************************************************************************
          buf.skip(); // 0x00 OkPacket Header
          buf.skip(buf.readLengthNotNull()); // affectedRows
          buf.skip(buf.readLengthNotNull());
          // insertId
          context.setServerStatus(buf.readShort());
          break authentication_loop;

        default:
          throw context
              .getExceptionFactory()
              .create(
                  "unexpected data during authentication (header=" + (buf.getUnsignedByte()),
                  "08000");
      }
    }
    writer.permitTrace(true);
  }

  public static Credential loadCredential(
      CredentialPlugin credentialPlugin, Configuration configuration, HostAddress hostAddress)
      throws SQLException {
    if (credentialPlugin != null) {
      return credentialPlugin.initialize(configuration, configuration.user(), hostAddress).get();
    }
    return new Credential(configuration.user(), configuration.password());
  }

  public static SSLSocket sslWrapper(
      final HostAddress hostAddress,
      final Socket socket,
      long clientCapabilities,
      final byte exchangeCharset,
      Context context,
      Writer writer)
      throws SQLException, IOException {

    Configuration conf = context.getConf();
    if (conf.sslMode() != SslMode.DISABLE) {

      if ((context.getServerCapabilities() & Capabilities.SSL) == 0) {
        throw context
            .getExceptionFactory()
            .create("Trying to connect with ssl, but ssl not enabled in the server", "08000");
      }

      clientCapabilities |= Capabilities.SSL;
      SslRequestPacket.create(clientCapabilities, exchangeCharset).encode(writer, context);

      TlsSocketPlugin socketPlugin = TlsSocketPluginLoader.get(conf.tlsSocketType());
      SSLSocketFactory sslSocketFactory =
          socketPlugin.getSocketFactory(conf, context.getExceptionFactory());
      SSLSocket sslSocket = socketPlugin.createSocket(socket, sslSocketFactory);

      enabledSslProtocolSuites(sslSocket, conf);
      enabledSslCipherSuites(sslSocket, conf);

      sslSocket.setUseClientMode(true);
      sslSocket.startHandshake();

      // perform hostname verification
      // (rfc2818 indicate that if "client has external information as to the expected identity of
      // the server, the hostname check MAY be omitted")
      if (conf.sslMode() == SslMode.VERIFY_FULL && hostAddress != null) {

        SSLSession session = sslSocket.getSession();
        try {
          socketPlugin.verify(hostAddress.host, session, context.getThreadId());
        } catch (SSLException ex) {
          throw context
              .getExceptionFactory()
              .create(
                  "SSL hostname verification failed : "
                      + ex.getMessage()
                      + "\nThis verification can be disabled using the sslMode to VERIFY_CA "
                      + "but won't prevent man-in-the-middle attacks anymore",
                  "08006");
        }
      }
      return sslSocket;
    }
    return null;
  }

  /**
   * Return possible protocols : values of option enabledSslProtocolSuites is set, or default to
   * "TLSv1,TLSv1.1". MariaDB versions &ge; 10.0.15 and &ge; 5.5.41 supports TLSv1.2 if compiled
   * with openSSL (default). MySQL's community versions &ge; 5.7.10 is compiled with yaSSL, so max
   * TLS is TLSv1.1.
   *
   * @param sslSocket current sslSocket
   * @throws SQLException if protocol isn't a supported protocol
   */
  static void enabledSslProtocolSuites(SSLSocket sslSocket, Configuration conf)
      throws SQLException {
    if (conf.enabledSslProtocolSuites() != null) {
      List<String> possibleProtocols = Arrays.asList(sslSocket.getSupportedProtocols());
      String[] protocols = conf.enabledSslProtocolSuites().split("[,;\\s]+");
      for (String protocol : protocols) {
        if (!possibleProtocols.contains(protocol)) {
          throw new SQLException(
              "Unsupported SSL protocol '"
                  + protocol
                  + "'. Supported protocols : "
                  + possibleProtocols.toString().replace("[", "").replace("]", ""));
        }
      }
      sslSocket.setEnabledProtocols(protocols);
    }
  }

  /**
   * Set ssl socket cipher according to options.
   *
   * @param sslSocket current ssl socket
   * @param conf configuration
   * @throws SQLException if a cipher isn't known
   */
  static void enabledSslCipherSuites(SSLSocket sslSocket, Configuration conf) throws SQLException {
    if (conf.enabledSslCipherSuites() != null) {
      List<String> possibleCiphers = Arrays.asList(sslSocket.getSupportedCipherSuites());
      String[] ciphers = conf.enabledSslCipherSuites().split("[,;\\s]+");
      for (String cipher : ciphers) {
        if (!possibleCiphers.contains(cipher)) {
          throw new SQLException(
              "Unsupported SSL cipher '"
                  + cipher
                  + "'. Supported ciphers : "
                  + possibleCiphers.toString().replace("[", "").replace("]", ""));
        }
      }
      sslSocket.setEnabledCipherSuites(ciphers);
    }
  }
}
