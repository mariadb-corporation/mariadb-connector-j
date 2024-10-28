// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.List;
import javax.net.SocketFactory;
import javax.net.ssl.*;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.SocketHelper;
import org.mariadb.jdbc.client.socket.impl.SocketHandlerFunction;
import org.mariadb.jdbc.client.socket.impl.SocketUtility;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.CredentialPlugin;
import org.mariadb.jdbc.util.ConfigurableSocketFactory;
import org.mariadb.jdbc.util.constants.Capabilities;

/** Connection creation helper class */
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
      try {
        @SuppressWarnings("unchecked")
        Class<SocketFactory> socketFactoryClass =
            (Class<SocketFactory>)
                Class.forName(socketFactoryName, false, ConnectionHelper.class.getClassLoader());
        if (!SocketFactory.class.isAssignableFrom(socketFactoryClass)) {
          throw new IOException(
              "Wrong Socket factory implementation '" + conf.socketFactory() + "'");
        }
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

  /**
   * Connect socket
   *
   * @param conf configuration
   * @param hostAddress host to connect
   * @return socket
   * @throws SQLException if hostname is required and not provided, or socket cannot be created
   */
  public static Socket connectSocket(final Configuration conf, final HostAddress hostAddress)
      throws SQLException {
    Socket socket;
    try {
      socket = createSocket(conf, hostAddress);
      SocketHelper.setSocketOption(conf, socket);
      if (!socket.isConnected()) {
        InetSocketAddress sockAddr =
            hostAddress.pipe == null && hostAddress.localSocket == null
                ? new InetSocketAddress(hostAddress.host, hostAddress.port)
                : null;
        socket.connect(sockAddr, conf.connectTimeout());
      }
      return socket;

    } catch (SocketTimeoutException ste) {
      throw new SQLTimeoutException(
          String.format("Socket timeout when connecting to %s. %s", hostAddress, ste.getMessage()),
          "08000",
          ste);

    } catch (IOException ioe) {

      throw new SQLNonTransientConnectionException(
          String.format("Socket fail to connect to %s. %s", hostAddress, ioe.getMessage()),
          "08000",
          ioe);
    }
  }

  /**
   * Initialize client capability according to configuration and server capabilities.
   *
   * @param configuration configuration
   * @param serverCapabilities server capabilities
   * @param hostAddress host address server
   * @return client capabilities
   */
  public static long initializeClientCapabilities(
      final Configuration configuration,
      final long serverCapabilities,
      final HostAddress hostAddress) {
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
            | Capabilities.CLIENT_SESSION_TRACK;

    if (Boolean.parseBoolean(
        configuration.nonMappedOptions().getProperty("enableBulkUnitResult", "true"))) {
      capabilities |= Capabilities.BULK_UNIT_RESULTS;
    }

    if (Boolean.parseBoolean(
        configuration.nonMappedOptions().getProperty("disableSessionTracking", "false"))) {
      capabilities &= ~Capabilities.CLIENT_SESSION_TRACK;
    }

    // since skipping metadata is only available when using binary protocol,
    // only set it when server permit it and using binary protocol
    if (configuration.useServerPrepStmts()
        && Boolean.parseBoolean(
            configuration.nonMappedOptions().getProperty("enableSkipMeta", "true"))) {
      capabilities |= Capabilities.CACHE_METADATA;
    }

    // remains for compatibility
    if (Boolean.parseBoolean(
        configuration.nonMappedOptions().getProperty("interactiveClient", "false"))) {
      capabilities |= Capabilities.CLIENT_INTERACTIVE;
    }

    if (configuration.useBulkStmts() || configuration.useBulkStmtsForInserts()) {
      capabilities |= Capabilities.STMT_BULK_OPERATIONS;
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

    // extendedTypeInfo is a technical option
    boolean extendedTypeInfo =
        Boolean.parseBoolean(
            configuration.nonMappedOptions().getProperty("extendedTypeInfo", "true"));
    if (extendedTypeInfo) {
      capabilities |= Capabilities.EXTENDED_METADATA;
    }

    // useEof is a technical option
    boolean deprecateEof =
        Boolean.parseBoolean(configuration.nonMappedOptions().getProperty("deprecateEof", "true"));
    if (deprecateEof) {
      capabilities |= Capabilities.CLIENT_DEPRECATE_EOF;
    }

    if (configuration.useCompression()) {
      capabilities |= Capabilities.COMPRESS;
    }

    // connect to database directly if not needed to be created, or if slave, since cannot be
    // created
    if (configuration.database() != null
        && (!configuration.createDatabaseIfNotExist()
            || (configuration.createDatabaseIfNotExist()
                && (hostAddress != null && !hostAddress.primary)))) {
      capabilities |= Capabilities.CONNECT_WITH_DB;
    }
    SslMode sslMode = hostAddress.sslMode == null ? configuration.sslMode() : hostAddress.sslMode;
    if (sslMode != SslMode.DISABLE) {
      capabilities |= Capabilities.SSL;
    }
    return capabilities & serverCapabilities;
  }

  /**
   * Load user/password plugin if configured to.
   *
   * @param credentialPlugin configuration credential plugin
   * @param configuration configuration
   * @param hostAddress current connection host address
   * @return credentials
   * @throws SQLException if configured credential plugin fail
   */
  public static Credential loadCredential(
      CredentialPlugin credentialPlugin, Configuration configuration, HostAddress hostAddress)
      throws SQLException {
    if (credentialPlugin != null) {
      return credentialPlugin.initialize(configuration, configuration.user(), hostAddress).get();
    }
    return new Credential(configuration.user(), configuration.password());
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
