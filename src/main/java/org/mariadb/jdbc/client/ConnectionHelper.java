/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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

package org.mariadb.jdbc.client;

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
import org.mariadb.jdbc.SslMode;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;
import org.mariadb.jdbc.client.socket.SocketHandlerFunction;
import org.mariadb.jdbc.client.socket.SocketUtility;
import org.mariadb.jdbc.message.client.SslRequestPacket;
import org.mariadb.jdbc.message.server.AuthSwitchPacket;
import org.mariadb.jdbc.message.server.ErrorPacket;
import org.mariadb.jdbc.message.server.InitialHandshakePacket;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPlugin;
import org.mariadb.jdbc.plugin.authentication.AuthenticationPluginLoader;
import org.mariadb.jdbc.plugin.credential.Credential;
import org.mariadb.jdbc.plugin.credential.CredentialPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPlugin;
import org.mariadb.jdbc.plugin.tls.TlsSocketPluginLoader;
import org.mariadb.jdbc.util.ConfigurableSocketFactory;
import org.mariadb.jdbc.util.constants.Capabilities;

public class ConnectionHelper {

  private static final SocketHandlerFunction socketHandler;

  static {
    SocketHandlerFunction init;
    try {
      init = SocketUtility.getSocketHandler();
    } catch (Throwable t) {
      SocketHandlerFunction defaultSocketHandler = (conf, host) -> standardSocket(conf, host);
      init = defaultSocketHandler;
    }
    socketHandler = init;
  }

  /**
   * Create socket accordingly to options.
   *
   * @param conf Url options
   * @param host hostName ( mandatory only for named pipe)
   * @return a nex socket
   * @throws IOException if connection error occur
   */
  public static Socket createSocket(Configuration conf, String host) throws IOException {
    return socketHandler.apply(conf, host);
  }

  /**
   * Use standard socket implementation.
   *
   * @param conf url options
   * @param host host to connect
   * @return socket
   * @throws IOException in case of error establishing socket.
   */
  public static Socket standardSocket(Configuration conf, String host) throws IOException {
    SocketFactory socketFactory;
    String socketFactoryName = conf.socketFactory();
    if (socketFactoryName != null) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends SocketFactory> socketFactoryClass =
            (Class<? extends SocketFactory>) Class.forName(socketFactoryName);
        if (socketFactoryClass != null) {
          Constructor<? extends SocketFactory> constructor = socketFactoryClass.getConstructor();
          socketFactory = constructor.newInstance();
          if (socketFactoryClass.isInstance(ConfigurableSocketFactory.class)) {
            ((ConfigurableSocketFactory) socketFactory).setConfiguration(conf, host);
          }
          return socketFactory.createSocket();
        }
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

  public static Socket createSocket(final String host, final int port, final Configuration conf)
      throws SQLException {
    Socket socket;
    try {
      socket = createSocket(conf, host);
      socket.setTcpNoDelay(true);

      socket.setSoTimeout(conf.socketTimeout());
      if (conf.tcpKeepAlive()) {
        socket.setKeepAlive(true);
      }
      if (conf.tcpAbortiveClose()) {
        socket.setSoLinger(true, 0);
      }

      // Bind the socket to a particular interface if the connection property
      // localSocketAddress has been defined.
      if (conf.localSocketAddress() != null) {
        InetSocketAddress localAddress = new InetSocketAddress(conf.localSocketAddress(), 0);
        socket.bind(localAddress);
      }

      if (!socket.isConnected()) {
        InetSocketAddress sockAddr = conf.pipe() == null ? new InetSocketAddress(host, port) : null;
        socket.connect(sockAddr, conf.connectTimeout());
      }
      return socket;

    } catch (IOException ioe) {
      throw new SQLNonTransientConnectionException(
          String.format(
              "Socket fail to connect to host:%s, port:%s. %s", host, port, ioe.getMessage()),
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
            | Capabilities.MARIADB_CLIENT_STMT_BULK_OPERATIONS;

    if (!configuration.useAffectedRows()) {
      capabilities |= Capabilities.FOUND_ROWS;
    }

    if (configuration.allowMultiQueries() || (configuration.rewriteBatchedStatements())) {
      capabilities |= Capabilities.MULTI_STATEMENTS;
    }

    if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) != 0) {
      capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
    }

    if (configuration.useCompression() && ((serverCapabilities & Capabilities.COMPRESS) != 0)) {
      capabilities |= Capabilities.COMPRESS;
    }

    if (!configuration.database().isEmpty()) {
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
    if (handshake.getMajorServerVersion() == 5 && handshake.getMinorServerVersion() <= 1) {
      // 5.1 version doesn't know 4 bytes utf8
      return (byte) 33; // utf8_general_ci
    }
    return (byte) 224; // UTF8MB4_UNICODE_CI;
  }

  public static void authenticationHandler(
      Credential credential, PacketWriter writer, PacketReader reader, Context context)
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
          AuthSwitchPacket authSwitchPacket = AuthSwitchPacket.decode(buf, context);
          AuthenticationPlugin authenticationPlugin =
              AuthenticationPluginLoader.get(authSwitchPacket.getPlugin());

          authenticationPlugin.initialize(credential.getPassword(), context.getSeed(), conf);
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
      final String host,
      final Socket socket,
      long clientCapabilities,
      final byte exchangeCharset,
      Context context,
      PacketWriter writer)
      throws SQLException, IOException {

    Configuration conf = context.getConf();
    if (conf.sslMode() != SslMode.DISABLE) {

      if ((context.getServerCapabilities() & Capabilities.SSL) == 0) {
        context
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
      if (conf.sslMode() == SslMode.VERIFY_FULL) {
        SSLSession session = sslSocket.getSession();
        try {
          socketPlugin.verify(host, session, conf, context.getThreadId());
        } catch (SSLException ex) {
          throw context
              .getExceptionFactory()
              .create(
                  "SSL hostname verification failed : "
                      + ex.getMessage()
                      + "\nThis verification can be disabled using the option \"disableSslHostnameVerification\" "
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
   * with openSSL (default). MySQL community versions &ge; 5.7.10 is compile with yaSSL, so max TLS
   * is TLSv1.1.
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
