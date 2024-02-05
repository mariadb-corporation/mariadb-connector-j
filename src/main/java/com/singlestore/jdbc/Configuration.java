// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import com.singlestore.jdbc.export.HaMode;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.plugin.Codec;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import com.singlestore.jdbc.util.options.OptionAliases;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * parse and verification of URL.
 *
 * <p>basic syntax :<br>
 * {@code
 * jdbc:singlestore:[replication:|failover|loadbalance:]//<hostDescription>[,<hostDescription>]/[database>]
 * [?<key1>=<value1>[&<key2>=<value2>]] }
 *
 * <p>hostDescription:<br>
 * - simple :<br>
 * {@code <host>:<portnumber>}<br>
 * (for example localhost:3306)<br>
 * <br>
 * - complex :<br>
 * {@code address=[(type=(master|slave))][(port=<portnumber>)](host=<host>)}<br>
 * <br>
 * <br>
 * type is by default master<br>
 * port is by default 3306<br>
 *
 * <p>host can be dns name, ipv4 or ipv6.<br>
 * in case of ipv6 and simple host description, the ip must be written inside bracket.<br>
 * exemple : {@code jdbc:singlestore://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 *
 * <p>Some examples :<br>
 * {@code jdbc:singlestore://localhost:3306/database?user=greg&password=pass}<br>
 * {@code
 * jdbc:singlestore://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}
 * <br>
 */
public class Configuration {

  private final Logger logger;

  // standard options
  private String user = null;
  private String password = null;
  private String database = null;
  private List<HostAddress> addresses = null;
  private HaMode haMode = HaMode.NONE;

  private String initialUrl = null;
  private Properties nonMappedOptions = null;

  // various
  private Boolean autocommit = null;
  private boolean createDatabaseIfNotExist = false;
  private String initSql = null;
  private TransactionIsolation transactionIsolation = TransactionIsolation.READ_COMMITTED;
  private int defaultFetchSize = 0;
  private int maxQuerySizeToLog = 1024;
  private Integer maxAllowedPacket = null;
  private String geometryDefaultType = null;
  private String restrictedAuth = null;
  // socket
  private String socketFactory = null;
  private int connectTimeout =
      DriverManager.getLoginTimeout() > 0 ? DriverManager.getLoginTimeout() * 1000 : 30_000;
  private String pipe = null;
  private String localSocket = null;
  private boolean tcpKeepAlive = true;
  private int tcpKeepIdle = 0;
  private int tcpKeepCount = 0;
  private int tcpKeepInterval = 0;
  private boolean tcpAbortiveClose = false;
  private String localSocketAddress = null;
  private int socketTimeout = 0;
  private boolean useReadAheadInput = false;
  private String tlsSocketType = null;

  // SSL
  private SslMode sslMode = SslMode.DISABLE;
  private String serverSslCert = null;
  private String trustStore = null;
  private String trustStorePassword = null;
  private String trustStoreType = null;
  private String keyStore = null;
  private String keyStorePassword = null;
  private String keyPassword = null;
  private String keyStoreType = null;
  private String enabledSslCipherSuites = null;
  private String enabledSslProtocolSuites = null;

  // protocol
  private boolean allowMultiQueries = false;
  private boolean allowLocalInfile = false;
  private boolean useCompression = false;
  private boolean useAffectedRows = false;
  private boolean disablePipeline = false;

  // prepare
  private boolean cachePrepStmts = true;
  private int prepStmtCacheSize = 250;
  private boolean useServerPrepStmts = false;

  // authentication
  private CredentialPlugin credentialType = null;
  private String sessionVariables = null;
  private String connectionAttributes = null;
  private String servicePrincipalName = null;
  private String jaasApplicationName = null;

  // meta
  private boolean blankTableNameMeta = false;
  private boolean tinyInt1isBit = true;
  private boolean transformedBitIsBoolean = false;
  private boolean yearIsDateType = true;
  private boolean dumpQueriesOnException = false;
  private boolean includeThreadDumpInDeadlockExceptions = false;

  // HA options
  private int retriesAllDown = 120;
  private boolean transactionReplay = false;
  private int transactionReplaySize = 64;

  // Pool options
  private boolean pool = false;
  private String poolName = null;
  private int maxPoolSize = 8;
  private int minPoolSize = 8;
  private int maxIdleTime = 600_000;
  private boolean registerJmxPool = true;
  private int poolValidMinDelay = 1000;
  private boolean useResetConnection = false;

  private Codec<?>[] codecs = null;

  private boolean useMysqlVersion = false;
  private boolean rewriteBatchedStatements = false;
  private String consoleLogLevel = null;
  private String consoleLogFilepath = null;
  private boolean printStackTrace = false;
  private Integer maxPrintStackSizeToLog = 10;

  private Configuration() {
    this.logger = Loggers.getLogger(Configuration.class);
  }

  private Configuration(
      String user,
      String password,
      String database,
      List<HostAddress> addresses,
      HaMode haMode,
      Properties nonMappedOptions,
      Boolean autocommit,
      boolean createDatabaseIfNotExist,
      String initSql,
      TransactionIsolation transactionIsolation,
      int defaultFetchSize,
      int maxQuerySizeToLog,
      Integer maxAllowedPacket,
      String geometryDefaultType,
      String restrictedAuth,
      String socketFactory,
      int connectTimeout,
      String pipe,
      String localSocket,
      boolean tcpKeepAlive,
      int tcpKeepIdle,
      int tcpKeepCount,
      int tcpKeepInterval,
      boolean tcpAbortiveClose,
      String localSocketAddress,
      int socketTimeout,
      boolean useReadAheadInput,
      String tlsSocketType,
      SslMode sslMode,
      String serverSslCert,
      String trustStore,
      String trustStorePassword,
      String trustStoreType,
      String keyStore,
      String keyStorePassword,
      String keyPassword,
      String keyStoreType,
      String enabledSslCipherSuites,
      String enabledSslProtocolSuites,
      boolean allowMultiQueries,
      boolean allowLocalInfile,
      boolean useCompression,
      boolean useAffectedRows,
      boolean disablePipeline,
      boolean cachePrepStmts,
      int prepStmtCacheSize,
      boolean useServerPrepStmts,
      CredentialPlugin credentialType,
      String sessionVariables,
      String connectionAttributes,
      String servicePrincipalName,
      String jaasApplicationName,
      boolean blankTableNameMeta,
      boolean tinyInt1isBit,
      boolean transformedBitIsBoolean,
      boolean yearIsDateType,
      boolean dumpQueriesOnException,
      boolean includeThreadDumpInDeadlockExceptions,
      int retriesAllDown,
      boolean transactionReplay,
      int transactionReplaySize,
      boolean pool,
      String poolName,
      int maxPoolSize,
      int minPoolSize,
      int maxIdleTime,
      boolean registerJmxPool,
      int poolValidMinDelay,
      boolean useResetConnection,
      boolean useMysqlVersion,
      boolean rewriteBatchedStatements,
      String consoleLogLevel,
      String consoleLogFilepath,
      boolean printStackTrace,
      int maxPrintStackSizeToLog) {
    this.user = user;
    this.password = password;
    this.database = database;
    this.addresses = addresses;
    this.haMode = haMode;
    this.nonMappedOptions = nonMappedOptions;
    this.autocommit = autocommit;
    this.createDatabaseIfNotExist = createDatabaseIfNotExist;
    this.initSql = initSql;
    this.transactionIsolation = transactionIsolation;
    this.defaultFetchSize = defaultFetchSize;
    this.maxQuerySizeToLog = maxQuerySizeToLog;
    this.maxAllowedPacket = maxAllowedPacket;
    this.geometryDefaultType = geometryDefaultType;
    this.restrictedAuth = restrictedAuth;
    this.socketFactory = socketFactory;
    this.connectTimeout = connectTimeout;
    this.pipe = pipe;
    this.localSocket = localSocket;
    this.tcpKeepAlive = tcpKeepAlive;
    this.tcpKeepIdle = tcpKeepIdle;
    this.tcpKeepCount = tcpKeepCount;
    this.tcpKeepInterval = tcpKeepInterval;
    this.tcpAbortiveClose = tcpAbortiveClose;
    this.localSocketAddress = localSocketAddress;
    this.socketTimeout = socketTimeout;
    this.useReadAheadInput = useReadAheadInput;
    this.tlsSocketType = tlsSocketType;
    this.sslMode = sslMode;
    this.serverSslCert = serverSslCert;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
    this.trustStoreType = trustStoreType;
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.keyPassword = keyPassword;
    this.keyStoreType = keyStoreType;
    this.enabledSslCipherSuites = enabledSslCipherSuites;
    this.enabledSslProtocolSuites = enabledSslProtocolSuites;
    this.allowMultiQueries = allowMultiQueries;
    this.allowLocalInfile = allowLocalInfile;
    this.useCompression = useCompression;
    this.useAffectedRows = useAffectedRows;
    this.disablePipeline = disablePipeline;
    this.cachePrepStmts = cachePrepStmts;
    this.prepStmtCacheSize = prepStmtCacheSize;
    this.useServerPrepStmts = useServerPrepStmts;
    this.credentialType = credentialType;
    this.sessionVariables = sessionVariables;
    this.connectionAttributes = connectionAttributes;
    this.servicePrincipalName = servicePrincipalName;
    this.jaasApplicationName = jaasApplicationName;
    this.blankTableNameMeta = blankTableNameMeta;
    this.tinyInt1isBit = tinyInt1isBit;
    this.transformedBitIsBoolean = transformedBitIsBoolean;
    this.yearIsDateType = yearIsDateType;
    this.dumpQueriesOnException = dumpQueriesOnException;
    this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
    this.retriesAllDown = retriesAllDown;
    this.transactionReplay = transactionReplay;
    this.transactionReplaySize = transactionReplaySize;
    this.pool = pool;
    this.poolName = poolName;
    this.maxPoolSize = maxPoolSize;
    this.minPoolSize = minPoolSize;
    this.maxIdleTime = maxIdleTime;
    this.registerJmxPool = registerJmxPool;
    this.poolValidMinDelay = poolValidMinDelay;
    this.useResetConnection = useResetConnection;
    this.useMysqlVersion = useMysqlVersion;
    this.rewriteBatchedStatements = rewriteBatchedStatements;
    this.consoleLogLevel = consoleLogLevel;
    this.consoleLogFilepath = consoleLogFilepath;
    this.printStackTrace = printStackTrace;
    this.maxPrintStackSizeToLog = maxPrintStackSizeToLog;
    this.initialUrl = buildUrl(this);
    this.logger = Loggers.getLogger(Configuration.class);
  }

  private Configuration(
      String database,
      List<HostAddress> addresses,
      HaMode haMode,
      String user,
      String password,
      String enabledSslProtocolSuites,
      String socketFactory,
      Integer connectTimeout,
      String pipe,
      String localSocket,
      Boolean tcpKeepAlive,
      Integer tcpKeepIdle,
      Integer tcpKeepCount,
      Integer tcpKeepInterval,
      Boolean tcpAbortiveClose,
      String localSocketAddress,
      Integer socketTimeout,
      Boolean allowMultiQueries,
      Boolean allowLocalInfile,
      Boolean useCompression,
      Boolean blankTableNameMeta,
      String credentialType,
      String sslMode,
      String transactionIsolation,
      String enabledSslCipherSuites,
      String sessionVariables,
      Boolean tinyInt1isBit,
      Boolean transformedBitIsBoolean,
      Boolean yearIsDateType,
      Boolean dumpQueriesOnException,
      Integer prepStmtCacheSize,
      Boolean useAffectedRows,
      Boolean disablePipeline,
      Boolean useServerPrepStmts,
      String connectionAttributes,
      Boolean autocommit,
      Boolean createDatabaseIfNotExist,
      String initSql,
      Boolean includeThreadDumpInDeadlockExceptions,
      String servicePrincipalName,
      String jaasApplicationName,
      Integer defaultFetchSize,
      String tlsSocketType,
      Integer maxQuerySizeToLog,
      Integer maxAllowedPacket,
      Integer retriesAllDown,
      Boolean pool,
      String poolName,
      Integer maxPoolSize,
      Integer minPoolSize,
      Integer maxIdleTime,
      Boolean registerJmxPool,
      Integer poolValidMinDelay,
      Boolean useResetConnection,
      String serverSslCert,
      String trustStore,
      String trustStorePassword,
      String trustStoreType,
      String keyStore,
      String keyStorePassword,
      String keyPassword,
      String keyStoreType,
      Boolean useReadAheadInput,
      Boolean cachePrepStmts,
      Boolean transactionReplay,
      Integer transactionReplaySize,
      String geometryDefaultType,
      String restrictedAuth,
      Properties nonMappedOptions,
      Boolean useMysqlVersion,
      Boolean rewriteBatchedStatements,
      String consoleLogLevel,
      String consoleLogFilepath,
      Boolean printStackTrace,
      Integer maxPrintStackSizeToLog)
      throws SQLException {
    this.consoleLogLevel = consoleLogLevel;
    this.consoleLogFilepath = consoleLogFilepath;
    if (printStackTrace != null) this.printStackTrace = printStackTrace;
    if (maxPrintStackSizeToLog != null && maxPrintStackSizeToLog > 0)
      this.maxPrintStackSizeToLog = maxPrintStackSizeToLog;
    Loggers.resetLoggerFactoryProperties(
        this.consoleLogLevel,
        this.consoleLogFilepath,
        this.printStackTrace,
        this.maxPrintStackSizeToLog);
    this.logger = Loggers.getLogger(Configuration.class);
    this.database = database;
    this.addresses = addresses;
    this.nonMappedOptions = nonMappedOptions;
    if (haMode != null) this.haMode = haMode;
    this.credentialType = CredentialPluginLoader.get(credentialType);
    this.user = user;
    this.password = password;
    this.enabledSslProtocolSuites = enabledSslProtocolSuites;
    this.socketFactory = socketFactory;
    if (connectTimeout != null) this.connectTimeout = connectTimeout;
    this.pipe = pipe;
    this.localSocket = localSocket;
    if (tcpKeepAlive != null) this.tcpKeepAlive = tcpKeepAlive;
    if (tcpKeepIdle != null) this.tcpKeepIdle = tcpKeepIdle;
    if (tcpKeepCount != null) this.tcpKeepCount = tcpKeepCount;
    if (tcpKeepInterval != null) this.tcpKeepInterval = tcpKeepInterval;
    if (tcpAbortiveClose != null) this.tcpAbortiveClose = tcpAbortiveClose;
    this.localSocketAddress = localSocketAddress;
    if (socketTimeout != null) this.socketTimeout = socketTimeout;
    if (allowMultiQueries != null) this.allowMultiQueries = allowMultiQueries;
    if (allowLocalInfile != null) this.allowLocalInfile = allowLocalInfile;
    if (useCompression != null) this.useCompression = useCompression;
    if (blankTableNameMeta != null) this.blankTableNameMeta = blankTableNameMeta;
    if (this.credentialType != null
        && this.credentialType.mustUseSsl()
        && (sslMode == null || SslMode.from(sslMode) == SslMode.DISABLE)) {
      logger.warn(
          "Credential type '"
              + this.credentialType.type()
              + "' is required to be used with SSL. "
              + "Enabling SSL.");
      this.sslMode = SslMode.VERIFY_FULL;
    } else {
      this.sslMode = sslMode != null ? SslMode.from(sslMode) : SslMode.DISABLE;
    }
    if (transactionIsolation != null)
      this.transactionIsolation = TransactionIsolation.from(transactionIsolation);
    this.enabledSslCipherSuites = enabledSslCipherSuites;
    this.sessionVariables = sessionVariables;
    if (tinyInt1isBit != null) this.tinyInt1isBit = tinyInt1isBit;
    if (transformedBitIsBoolean != null) this.transformedBitIsBoolean = transformedBitIsBoolean;
    if (yearIsDateType != null) this.yearIsDateType = yearIsDateType;
    if (dumpQueriesOnException != null) this.dumpQueriesOnException = dumpQueriesOnException;
    if (prepStmtCacheSize != null) this.prepStmtCacheSize = prepStmtCacheSize;
    if (useAffectedRows != null) this.useAffectedRows = useAffectedRows;
    if (disablePipeline != null) this.disablePipeline = disablePipeline;
    if (useServerPrepStmts != null) this.useServerPrepStmts = useServerPrepStmts;
    this.connectionAttributes = connectionAttributes;
    if (autocommit != null) this.autocommit = autocommit;
    if (createDatabaseIfNotExist != null) this.createDatabaseIfNotExist = createDatabaseIfNotExist;
    if (initSql != null) this.initSql = initSql;
    if (includeThreadDumpInDeadlockExceptions != null)
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
    if (servicePrincipalName != null) this.servicePrincipalName = servicePrincipalName;
    if (jaasApplicationName != null) this.jaasApplicationName = jaasApplicationName;
    if (defaultFetchSize != null) this.defaultFetchSize = defaultFetchSize;
    if (tlsSocketType != null) this.tlsSocketType = tlsSocketType;
    if (maxQuerySizeToLog != null) this.maxQuerySizeToLog = maxQuerySizeToLog;
    if (maxAllowedPacket != null) this.maxAllowedPacket = maxAllowedPacket;
    if (retriesAllDown != null) this.retriesAllDown = retriesAllDown;
    if (pool != null) this.pool = pool;
    if (poolName != null) this.poolName = poolName;
    if (maxPoolSize != null) this.maxPoolSize = maxPoolSize;
    // if min pool size default to maximum pool size if not set
    if (minPoolSize != null) {
      this.minPoolSize = minPoolSize;
    } else {
      this.minPoolSize = this.maxPoolSize;
    }

    if (maxIdleTime != null) {
      if (maxIdleTime < 2) {
        throw new IllegalArgumentException(
            String.format("Wrong argument value '%d' for maxIdleTime, must be >= 2", maxIdleTime));
      }
      this.maxIdleTime = maxIdleTime;
    }
    if (registerJmxPool != null) this.registerJmxPool = registerJmxPool;
    if (poolValidMinDelay != null) this.poolValidMinDelay = poolValidMinDelay;
    if (useResetConnection != null) this.useResetConnection = useResetConnection;
    if (useReadAheadInput != null) this.useReadAheadInput = useReadAheadInput;
    if (cachePrepStmts != null) this.cachePrepStmts = cachePrepStmts;
    if (transactionReplay != null) this.transactionReplay = transactionReplay;
    if (transactionReplaySize != null) this.transactionReplaySize = transactionReplaySize;
    if (geometryDefaultType != null) this.geometryDefaultType = geometryDefaultType;
    if (restrictedAuth != null) this.restrictedAuth = restrictedAuth;
    if (serverSslCert != null) this.serverSslCert = serverSslCert;
    if (trustStore != null) this.trustStore = trustStore;
    if (trustStorePassword != null) this.trustStorePassword = trustStorePassword;
    if (trustStoreType != null) this.trustStoreType = trustStoreType;
    if (keyStore != null) this.keyStore = keyStore;
    if (keyStorePassword != null) this.keyStorePassword = keyStorePassword;
    if (keyPassword != null) this.keyPassword = keyPassword;
    if (keyStoreType != null) this.keyStoreType = keyStoreType;
    if (useMysqlVersion != null) this.useMysqlVersion = useMysqlVersion;
    if (rewriteBatchedStatements != null) this.rewriteBatchedStatements = rewriteBatchedStatements;

    // *************************************************************
    // host primary check
    // *************************************************************
    boolean first = true;
    for (HostAddress host : addresses) {
      boolean primary = haMode != HaMode.REPLICATION || first;
      if (host.primary == null) {
        host.primary = primary;
      }
      first = false;
    }

    // *************************************************************
    // option value verification
    // *************************************************************

    // int fields must all be positive
    Field[] fields = Configuration.class.getDeclaredFields();
    try {
      for (Field field : fields) {
        if (field.getType().equals(int.class)) {
          int val = field.getInt(this);
          if (val < 0) {
            throw new SQLException(
                String.format("Value for %s must be >= 1 (value is %s)", field.getName(), val));
          }
        }
      }
    } catch (IllegalArgumentException | IllegalAccessException ie) {
      // eat
    }
  }

  /**
   * Tell if the driver accepts url string. (Correspond to interface java.jdbc.Driver.acceptsURL()
   * method)
   *
   * @param url url String
   * @return true if url string correspond.
   */
  public static boolean acceptsUrl(String url) {
    return url != null && url.startsWith("jdbc:singlestore:");
  }

  public static Configuration parse(final String url) throws SQLException {
    return parse(url, new Properties());
  }

  /**
   * Parse url connection string with additional properties.
   *
   * @param url connection string
   * @param prop properties
   * @return UrlParser instance
   * @throws SQLException if parsing exception occur
   */
  public static Configuration parse(final String url, Properties prop) throws SQLException {
    if (acceptsUrl(url)) {
      return parseInternal(url, (prop == null) ? new Properties() : prop);
    }
    return null;
  }

  /**
   * Parses the connection URL in order to set the UrlParser instance with all the information
   * provided through the URL.
   *
   * @param url connection URL
   * @param properties properties
   * @throws SQLException if format is incorrect
   */
  private static Configuration parseInternal(String url, Properties properties)
      throws SQLException {
    try {
      Builder builder = new Builder();
      int separator = url.indexOf("//");
      if (separator == -1) {
        throw new IllegalArgumentException(
            "url parsing error : '//' is not present in the url " + url);
      }
      builder.haMode(parseHaMode(url, separator));

      String urlSecondPart = url.substring(separator + 2);
      int dbIndex = urlSecondPart.indexOf("/");
      int paramIndex = urlSecondPart.indexOf("?");

      String hostAddressesString;
      String additionalParameters;
      if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
        hostAddressesString = urlSecondPart.substring(0, paramIndex);
        additionalParameters = urlSecondPart.substring(paramIndex);
      } else if (dbIndex < paramIndex || dbIndex > paramIndex) {
        hostAddressesString = urlSecondPart.substring(0, dbIndex);
        additionalParameters = urlSecondPart.substring(dbIndex);
      } else {
        hostAddressesString = urlSecondPart;
        additionalParameters = null;
      }

      if (additionalParameters != null) {
        int optIndex = additionalParameters.indexOf("?");
        String database;
        if (optIndex < 0) {
          database = (additionalParameters.length() > 1) ? additionalParameters.substring(1) : null;
        } else {
          if (optIndex == 0) {
            database = null;
          } else {
            database = additionalParameters.substring(1, optIndex);
            if (database.isEmpty()) database = null;
          }
          String urlParameters = additionalParameters.substring(optIndex + 1);
          if (urlParameters != null && !urlParameters.isEmpty()) {
            String[] parameters = urlParameters.split("&");
            for (String parameter : parameters) {
              int pos = parameter.indexOf('=');
              if (pos == -1) {
                properties.setProperty(parameter, "");
              } else {
                properties.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
              }
            }
          }
        }
        builder.database(database);
      } else {
        builder.database(null);
      }

      mapPropertiesToOption(builder, properties);
      builder._addresses = HostAddress.parse(hostAddressesString, builder._haMode);
      return builder.build();

    } catch (IllegalArgumentException i) {
      throw new SQLException("error parsing url : " + i.getMessage(), i);
    }
  }

  private static void mapPropertiesToOption(Builder builder, Properties properties) {
    Properties nonMappedOptions = new Properties();

    try {
      // Option object is already initialized to default values.
      // loop on properties,
      // - check DefaultOption to check that property value correspond to type (and range)
      // - set values
      for (final Object keyObj : properties.keySet()) {
        String realKey =
            OptionAliases.OPTIONS_ALIASES.get(keyObj.toString().toLowerCase(Locale.ROOT));
        if (realKey == null) realKey = keyObj.toString();
        final Object propertyValue = properties.get(keyObj);
        if (propertyValue != null && realKey != null) {
          boolean used = false;
          for (Field field : Builder.class.getDeclaredFields()) {
            if (realKey.toLowerCase(Locale.ROOT).equals(field.getName().toLowerCase(Locale.ROOT))) {
              field.setAccessible(true);
              used = true;

              if (field.getGenericType().equals(String.class)
                  && !propertyValue.toString().isEmpty()) {
                field.set(builder, propertyValue);
              } else if (field.getGenericType().equals(Boolean.class)) {
                switch (propertyValue.toString().toLowerCase()) {
                  case "":
                  case "1":
                  case "true":
                    field.set(builder, Boolean.TRUE);
                    break;

                  case "0":
                  case "false":
                    field.set(builder, Boolean.FALSE);
                    break;

                  default:
                    throw new IllegalArgumentException(
                        String.format(
                            "Optional parameter %s must be boolean (true/false or 0/1) was '%s'",
                            keyObj, propertyValue));
                }
              } else if (field.getGenericType().equals(Integer.class)) {
                try {
                  final Integer value = Integer.parseInt(propertyValue.toString());
                  field.set(builder, value);
                } catch (NumberFormatException n) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Optional parameter %s must be Integer, was '%s'",
                          keyObj, propertyValue));
                }
              }
            }
          }
          if (!used) nonMappedOptions.put(realKey, propertyValue);
        }
      }

      // for compatibility with 2.x
      if (isSet("useSsl", nonMappedOptions) || isSet("useSSL", nonMappedOptions)) {
        if (isSet("trustServerCertificate", nonMappedOptions)) {
          builder.sslMode("trust");
        } else if (isSet("disableSslHostnameVerification", nonMappedOptions)) {
          builder.sslMode("verify-ca");
        } else {
          builder.sslMode("verify-full");
        }
      }
    } catch (IllegalAccessException | SecurityException s) {
      throw new IllegalArgumentException("Unexpected error", s);
    }
    builder._nonMappedOptions = nonMappedOptions;
  }

  private static boolean isSet(String key, Properties nonMappedOptions) {
    String value = nonMappedOptions.getProperty(key);
    return value != null && (value.equals("1") || value.equals("true") || value.isEmpty());
  }

  private static HaMode parseHaMode(String url, int separator) {
    // parser is sure to have at least 2 colon, since jdbc:[mysql|singlestore]: is tested.
    int firstColonPos = url.indexOf(':');
    int secondColonPos = url.indexOf(':', firstColonPos + 1);
    int thirdColonPos = url.indexOf(':', secondColonPos + 1);

    if (thirdColonPos > separator || thirdColonPos == -1) {
      if (secondColonPos == separator - 1) {
        return HaMode.NONE;
      }
      thirdColonPos = separator;
    }

    try {
      String haModeString = url.substring(secondColonPos + 1, thirdColonPos);
      if ("FAILOVER".equalsIgnoreCase(haModeString)) {
        haModeString = "LOADBALANCE";
      }
      return HaMode.from(haModeString);
    } catch (IllegalArgumentException i) {
      throw new IllegalArgumentException(
          "wrong failover parameter format in connection String " + url);
    }
  }

  /**
   * Permit to have string information on how string is parsed. example :
   * Configuration.toConf("jdbc:singlestore://localhost/test") will return a String containing:
   * <code>
   * Configuration:
   *  * resulting Url : jdbc:singlestore://localhost/test
   * Unknown options : None
   *
   * Non default options :
   *  * database : test
   *
   * default options :
   *  * user : null
   *  ...
   * </code>
   *
   * @param url url string
   * @return string describing the configuration parsed from url
   * @throws SQLException if parsing fails
   */
  public static String toConf(String url) throws SQLException {
    Configuration conf = Configuration.parseInternal(url, new Properties());
    StringBuilder sb = new StringBuilder();
    StringBuilder sbUnknownOpts = new StringBuilder();

    if (conf.nonMappedOptions.isEmpty()) {
      sbUnknownOpts.append("None");
    } else {
      for (Map.Entry<Object, Object> entry : conf.nonMappedOptions.entrySet()) {
        sbUnknownOpts.append("\n * ").append(entry.getKey()).append(" : ").append(entry.getValue());
      }
    }
    sb.append("Configuration:")
        .append("\n * resulting Url : ")
        .append(conf.initialUrl)
        .append("\nUnknown options : ")
        .append(sbUnknownOpts)
        .append("\n")
        .append("\nNon default options : ");

    Configuration defaultConf = Configuration.parse("jdbc:singlestore://localhost/");
    StringBuilder sbDefaultOpts = new StringBuilder();
    StringBuilder sbDifferentOpts = new StringBuilder();
    try {
      List<String> propertyToSkip = Arrays.asList("initialUrl", "logger", "codecs", "$jacocoData");
      Field[] fields = Configuration.class.getDeclaredFields();
      Arrays.sort(fields, Comparator.comparing(Field::getName));

      for (Field field : fields) {
        if (!propertyToSkip.contains(field.getName())) {
          Object fieldValue = field.get(conf);
          if (fieldValue == null) {
            (Objects.equals(fieldValue, field.get(defaultConf)) ? sbDefaultOpts : sbDifferentOpts)
                .append("\n * ")
                .append(field.getName())
                .append(" : ")
                .append(fieldValue);
          } else {
            if (field.getName().equals("haMode")) {
              (Objects.equals(fieldValue, field.get(defaultConf)) ? sbDefaultOpts : sbDifferentOpts)
                  .append("\n * ")
                  .append(field.getName())
                  .append(" : ")
                  .append(fieldValue);
              continue;
            }
            switch (fieldValue.getClass().getSimpleName()) {
              case "String":
              case "Boolean":
              case "HaMode":
              case "Integer":
              case "SslMode":
                (Objects.equals(fieldValue, field.get(defaultConf))
                        ? sbDefaultOpts
                        : sbDifferentOpts)
                    .append("\n * ")
                    .append(field.getName())
                    .append(" : ")
                    .append(fieldValue);
                break;
              case "ArrayList":
                (Objects.equals(fieldValue.toString(), field.get(defaultConf).toString())
                        ? sbDefaultOpts
                        : sbDifferentOpts)
                    .append("\n * ")
                    .append(field.getName())
                    .append(" : ")
                    .append(fieldValue);
                break;
              case "Properties":
                break;
              default:
                throw new IllegalArgumentException(
                    "field type not expected for fields " + field.getName());
            }
          }
        }
      }

      String diff = sbDifferentOpts.toString();
      if (diff.isEmpty()) {
        sb.append("None\n");
      } else {
        sb.append(diff);
      }

      sb.append("\n\ndefault options :");
      String same = sbDefaultOpts.toString();
      if (same.isEmpty()) {
        sb.append("None\n");
      } else {
        sb.append(same);
      }

    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new IllegalArgumentException("Wrong parsing", e);
    }
    return sb.toString();
  }

  public Configuration clone(String username, String password) {
    return new Configuration(
        username != null && username.isEmpty() ? null : username,
        password != null && password.isEmpty() ? null : password,
        this.database,
        this.addresses,
        this.haMode,
        this.nonMappedOptions,
        this.autocommit,
        this.createDatabaseIfNotExist,
        this.initSql,
        this.transactionIsolation,
        this.defaultFetchSize,
        this.maxQuerySizeToLog,
        this.maxAllowedPacket,
        this.geometryDefaultType,
        this.restrictedAuth,
        this.socketFactory,
        this.connectTimeout,
        this.pipe,
        this.localSocket,
        this.tcpKeepAlive,
        this.tcpKeepIdle,
        this.tcpKeepCount,
        this.tcpKeepInterval,
        this.tcpAbortiveClose,
        this.localSocketAddress,
        this.socketTimeout,
        this.useReadAheadInput,
        this.tlsSocketType,
        this.sslMode,
        this.serverSslCert,
        this.trustStore,
        this.trustStorePassword,
        this.trustStoreType,
        this.keyStore,
        this.keyStorePassword,
        this.keyPassword,
        this.keyStoreType,
        this.enabledSslCipherSuites,
        this.enabledSslProtocolSuites,
        this.allowMultiQueries,
        this.allowLocalInfile,
        this.useCompression,
        this.useAffectedRows,
        this.disablePipeline,
        this.cachePrepStmts,
        this.prepStmtCacheSize,
        this.useServerPrepStmts,
        this.credentialType,
        this.sessionVariables,
        this.connectionAttributes,
        this.servicePrincipalName,
        this.jaasApplicationName,
        this.blankTableNameMeta,
        this.tinyInt1isBit,
        this.transformedBitIsBoolean,
        this.yearIsDateType,
        this.dumpQueriesOnException,
        this.includeThreadDumpInDeadlockExceptions,
        this.retriesAllDown,
        this.transactionReplay,
        this.transactionReplaySize,
        this.pool,
        this.poolName,
        this.maxPoolSize,
        this.minPoolSize,
        this.maxIdleTime,
        this.registerJmxPool,
        this.poolValidMinDelay,
        this.useResetConnection,
        this.useMysqlVersion,
        this.rewriteBatchedStatements,
        this.consoleLogLevel,
        this.consoleLogFilepath,
        this.printStackTrace,
        this.maxPrintStackSizeToLog);
  }

  /**
   * Connection default database
   *
   * @return database
   */
  public String database() {
    return database;
  }

  /**
   * addresses
   *
   * @return addresses
   */
  public List<HostAddress> addresses() {
    return addresses;
  }

  /**
   * High availability mode
   *
   * @return configuration HA mode
   */
  public HaMode haMode() {
    return haMode;
  }

  /**
   * credential plugin to use
   *
   * @return credential plugin to use, null of none
   */
  public CredentialPlugin credentialPlugin() {
    return credentialType;
  }

  /**
   * configuration user
   *
   * @return user
   */
  public String user() {
    return user;
  }

  /**
   * configuration password
   *
   * @return password
   */
  public String password() {
    return password;
  }

  /**
   * Configuration generated URL depending on current configuration option. Password will be hidden
   * by "***"
   *
   * @return generated url
   */
  public String initialUrl() {
    return initialUrl;
  }

  /**
   * server ssl certificate (file path / certificat content)
   *
   * @return server ssl certificate
   */
  public String serverSslCert() {
    return serverSslCert;
  }

  /**
   * trust store
   *
   * @return trust store
   */
  public String trustStore() {
    return trustStore;
  }

  /**
   * trust store password
   *
   * @return trust store password
   */
  public String trustStorePassword() {
    return trustStorePassword;
  }

  /**
   * trust store type
   *
   * @return trust store type
   */
  public String trustStoreType() {
    return trustStoreType;
  }

  /**
   * key store
   *
   * @return key store
   */
  public String keyStore() {
    return keyStore;
  }

  /**
   * key store password
   *
   * @return key store password
   */
  public String keyStorePassword() {
    return keyStorePassword;
  }

  /**
   * key store alias password
   *
   * @return key store alias password
   */
  public String keyPassword() {
    return keyPassword;
  }

  /**
   * key store type (to replace default javax.net.ssl.keyStoreType system property)
   *
   * @return key store type
   */
  public String keyStoreType() {
    return keyStoreType;
  }

  /**
   * permitted ssl protocol list (comma separated)
   *
   * @return enabled ssl protocol list
   */
  public String enabledSslProtocolSuites() {
    return enabledSslProtocolSuites;
  }

  /**
   * Socket factory class name
   *
   * @return socket factory
   */
  public String socketFactory() {
    return socketFactory;
  }

  /**
   * socket connect timeout
   *
   * @return connect timeout
   */
  public int connectTimeout() {
    return connectTimeout;
  }

  /**
   * Set connect timeout
   *
   * @param connectTimeout timeout value
   * @return current configuration
   */
  public Configuration connectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  /**
   * Pipe path
   *
   * @return pipe value
   */
  public String pipe() {
    return pipe;
  }

  /**
   * local socket configuration
   *
   * @return local socket path
   */
  public String localSocket() {
    return localSocket;
  }

  /**
   * socket tcp keep alive
   *
   * @return socket tcp keep alive value
   */
  public boolean tcpKeepAlive() {
    return tcpKeepAlive;
  }

  /**
   * socket tcp keep idle (java 11+ only)
   *
   * @return socket tcp keep idle
   */
  public int tcpKeepIdle() {
    return tcpKeepIdle;
  }

  /**
   * socket tcp keep count (java 11+ only)
   *
   * @return socket tcp keep count
   */
  public int tcpKeepCount() {
    return tcpKeepCount;
  }

  /**
   * socket tcp keep interval (java 11+ only)
   *
   * @return socket tcp keep interval
   */
  public int tcpKeepInterval() {
    return tcpKeepInterval;
  }

  /**
   * close using TCP abortive close (RST TCP packet, in place or FIN packet)
   *
   * @return close using TCP abortive close
   */
  public boolean tcpAbortiveClose() {
    return tcpAbortiveClose;
  }

  /**
   * local socket address path
   *
   * @return local socket address
   */
  public String localSocketAddress() {
    return localSocketAddress;
  }

  /**
   * socket timeout
   *
   * @return socket timeout
   */
  public int socketTimeout() {
    return socketTimeout;
  }

  /**
   * permit using multi queries command
   *
   * @return permit using multi queries command
   */
  public boolean allowMultiQueries() {
    return allowMultiQueries;
  }

  /**
   * permits LOAD LOCAL INFILE commands
   *
   * @return allow LOAD LOCAL INFILE
   */
  public boolean allowLocalInfile() {
    return allowLocalInfile;
  }

  /**
   * Enable compression if server has compression capability
   *
   * @return use compression
   */
  public boolean useCompression() {
    return useCompression;
  }

  /**
   * force returning blank table metadata (for old oracle compatibility)
   *
   * @return metadata table return blank
   */
  public boolean blankTableNameMeta() {
    return blankTableNameMeta;
  }

  /**
   * SSl mode
   *
   * @return ssl mode
   */
  public SslMode sslMode() {
    return sslMode;
  }

  /**
   * Default transaction isolation
   *
   * @return default transaction isolation.
   */
  public TransactionIsolation transactionIsolation() {
    return transactionIsolation;
  }

  /**
   * autorized cipher list.
   *
   * @return list of permitted ciphers
   */
  public String enabledSslCipherSuites() {
    return enabledSslCipherSuites;
  }

  /**
   * coma separated Session variable list
   *
   * @return session variable
   */
  public String sessionVariables() {
    return sessionVariables;
  }

  /**
   * Must tinyint be considered as Bit (TINYINT is always has reserved length = 4)
   *
   * @return true if tinyint must be considered as Bit
   */
  public boolean tinyInt1isBit() {
    return tinyInt1isBit;
  }

  /**
   * Must tinyint be considered as Boolean or Bit
   *
   * @return true if tinyint must be considered as Boolean
   */
  public boolean transformedBitIsBoolean() {
    return transformedBitIsBoolean;
  }

  /**
   * Must year be return by default as Date in result-set
   *
   * @return year is Date type
   */
  public boolean yearIsDateType() {
    return yearIsDateType;
  }

  /**
   * Must query by logged on exception.
   *
   * @return dump queries on exception
   */
  public boolean dumpQueriesOnException() {
    return dumpQueriesOnException;
  }

  /**
   * Prepare statement cache size.
   *
   * @return Prepare statement cache size
   */
  public int prepStmtCacheSize() {
    return prepStmtCacheSize;
  }

  /**
   * Use affected row
   *
   * @return use affected rows
   */
  public boolean useAffectedRows() {
    return useAffectedRows;
  }

  public boolean disablePipeline() {
    return disablePipeline;
  }

  /**
   * Use server prepared statement. IF false, using client prepared statement.
   *
   * @return use server prepared statement
   */
  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  public String connectionAttributes() {
    return connectionAttributes;
  }

  /**
   * Force session autocommit on connection creation
   *
   * @return autocommit forced value
   */
  public Boolean autocommit() {
    return autocommit;
  }

  public boolean includeThreadDumpInDeadlockExceptions() {
    return includeThreadDumpInDeadlockExceptions;
  }

  /**
   * create database if not exist
   *
   * @return create database if not exist
   */
  public boolean createDatabaseIfNotExist() {
    return createDatabaseIfNotExist;
  }

  /**
   * Execute initial command when connection is established
   *
   * @return initial SQL command
   */
  public String initSql() {
    return initSql;
  }

  public String servicePrincipalName() {
    return servicePrincipalName;
  }

  public String jaasApplicationName() {
    return jaasApplicationName;
  }

  public int defaultFetchSize() {
    return defaultFetchSize;
  }

  public Properties nonMappedOptions() {
    return nonMappedOptions;
  }

  public String tlsSocketType() {
    return tlsSocketType;
  }

  public int maxQuerySizeToLog() {
    return maxQuerySizeToLog;
  }

  /**
   * max_allowed_packet value to avoid sending packet with non supported size, droping the
   * connection without reason.
   *
   * @return max_allowed_packet value
   */
  public Integer maxAllowedPacket() {
    return maxAllowedPacket;
  }

  public int retriesAllDown() {
    return retriesAllDown;
  }

  public boolean pool() {
    return pool;
  }

  public String poolName() {
    return poolName;
  }

  public int maxPoolSize() {
    return maxPoolSize;
  }

  public int minPoolSize() {
    return minPoolSize;
  }

  public int maxIdleTime() {
    return maxIdleTime;
  }

  public boolean registerJmxPool() {
    return registerJmxPool;
  }

  public int poolValidMinDelay() {
    return poolValidMinDelay;
  }

  public boolean useResetConnection() {
    return useResetConnection;
  }

  public boolean useReadAheadInput() {
    return useReadAheadInput;
  }

  public boolean cachePrepStmts() {
    return cachePrepStmts;
  }

  public boolean transactionReplay() {
    return transactionReplay;
  }

  /**
   * transaction replay maximum number of saved command.
   *
   * @return transaction replay buffer size.
   */
  public int transactionReplaySize() {
    return transactionReplaySize;
  }

  public String geometryDefaultType() {
    return geometryDefaultType;
  }

  public String restrictedAuth() {
    return restrictedAuth;
  }

  public Codec<?>[] codecs() {
    return codecs;
  }

  public boolean useMysqlVersion() {
    return useMysqlVersion;
  }

  public boolean rewriteBatchedStatements() {
    return rewriteBatchedStatements;
  }

  public String getConsoleLogLevel() {
    return consoleLogLevel;
  }

  public String getConsoleLogFilepath() {
    return consoleLogFilepath;
  }

  public boolean printStackTrace() {
    return printStackTrace;
  }

  public int maxPrintStackSizeToLog() {
    return maxPrintStackSizeToLog;
  }

  /**
   * ToString implementation.
   *
   * @return String value
   */
  public String toString() {
    return initialUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Configuration that = (Configuration) o;

    return initialUrl.equals(that.initialUrl);
  }

  protected static String buildUrl(Configuration conf) {
    Configuration defaultConf = new Configuration();
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:singlestore:");
    if (conf.haMode != HaMode.NONE) {
      sb.append(conf.haMode.toString().toLowerCase(Locale.ROOT)).append(":");
    }
    sb.append("//");

    for (int i = 0; i < conf.addresses.size(); i++) {
      HostAddress hostAddress = conf.addresses.get(i);
      if (i > 0) {
        sb.append(",");
      }
      if ((conf.haMode == HaMode.NONE && hostAddress.primary)
          || (conf.haMode == HaMode.REPLICATION
              && ((i == 0 && hostAddress.primary) || (i != 0 && !hostAddress.primary)))) {
        sb.append(hostAddress.host);
        if (hostAddress.port != 3306) {
          sb.append(":").append(hostAddress.port);
        }
      } else {
        sb.append("address=(host=")
            .append(hostAddress.host)
            .append(")")
            .append("(port=")
            .append(hostAddress.port)
            .append(")");
        sb.append("(type=").append(hostAddress.primary ? "primary" : "replica").append(")");
      }
    }

    sb.append("/");
    if (conf.database != null) {
      sb.append(conf.database);
    }

    try {
      // Option object is already initialized to default values.
      // loop on properties,
      // - check DefaultOption to check that property value correspond to type (and range)
      // - set values
      boolean first = true;

      Field[] fields = Configuration.class.getDeclaredFields();
      for (Field field : fields) {
        if ("database".equals(field.getName())
            || "haMode".equals(field.getName())
            || "$jacocoData".equals(field.getName())
            || "addresses".equals(field.getName())) {
          continue;
        }
        Object obj = field.get(conf);

        if (obj != null && (!(obj instanceof Properties) || ((Properties) obj).size() > 0)) {
          if ("password".equals(field.getName())) {
            sb.append(first ? '?' : '&');
            first = false;
            sb.append(field.getName()).append('=');
            sb.append("***");
            continue;
          }
          if (field.getType().equals(String.class)) {
            sb.append(first ? '?' : '&');
            first = false;
            sb.append(field.getName()).append('=');
            sb.append((String) obj);
          } else if (field.getType().equals(boolean.class)) {
            boolean defaultValue = field.getBoolean(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append(obj);
            }
          } else if (field.getType().equals(int.class)) {
            try {
              int defaultValue = field.getInt(defaultConf);
              if (!obj.equals(defaultValue)) {
                sb.append(first ? '?' : '&');
                sb.append(field.getName()).append('=').append(obj);
                first = false;
              }
            } catch (IllegalAccessException n) {
              // eat
            }
          } else if (field.getType().equals(Properties.class)) {
            sb.append(first ? '?' : '&');
            first = false;
            boolean firstProp = true;
            Properties properties = (Properties) obj;
            for (Object key : properties.keySet()) {
              if (firstProp) {
                firstProp = false;
              } else {
                sb.append('&');
              }
              sb.append(key).append('=');
              sb.append(properties.get(key));
            }
          } else if (field.getType().equals(CredentialPlugin.class)) {
            Object defaultValue = field.get(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append(((CredentialPlugin) obj).type());
            }
          } else {
            Object defaultValue = field.get(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append(obj.toString());
            }
          }
        }
      }

    } catch (IllegalAccessException n) {
      n.printStackTrace();
    } catch (SecurityException s) {
      // only for jws, so never thrown
      throw new IllegalArgumentException("Security too restrictive : " + s.getMessage());
    }
    conf.loadCodecs();
    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  private void loadCodecs() {
    ServiceLoader<Codec> loader =
        ServiceLoader.load(Codec.class, Configuration.class.getClassLoader());
    List<Codec<?>> result = new ArrayList<>();
    loader.iterator().forEachRemaining(result::add);
    codecs = result.toArray(new Codec<?>[0]);
  }

  @Override
  public int hashCode() {
    return initialUrl.hashCode();
  }

  /** A builder for {@link Configuration} instances. */
  public static final class Builder implements Cloneable {

    private Properties _nonMappedOptions;
    private HaMode _haMode;
    private List<HostAddress> _addresses = new ArrayList<>();

    // standard options
    private String user;
    private String password;
    private String database;

    // various
    private Boolean autocommit;
    private Boolean createDatabaseIfNotExist;
    private String initSql;
    private Integer defaultFetchSize;
    private Integer maxQuerySizeToLog;
    private Integer maxAllowedPacket;
    private String geometryDefaultType;
    private String restrictedAuth;
    private String transactionIsolation;

    // socket
    private String socketFactory;
    private Integer connectTimeout;
    private String pipe;
    private String localSocket;
    private Boolean tcpKeepAlive;
    private Integer tcpKeepIdle;
    private Integer tcpKeepCount;
    private Integer tcpKeepInterval;
    private Boolean tcpAbortiveClose;
    private String localSocketAddress;
    private Integer socketTimeout;
    private Boolean useReadAheadInput;
    private String tlsSocketType;

    // SSL
    private String sslMode;
    private String serverSslCert;
    private String trustStore;
    private String trustStorePassword;
    private String trustStoreType;
    private String keyStore;
    private String keyStorePassword;
    private String keyPassword;
    private String keyStoreType;
    private String enabledSslCipherSuites;
    private String enabledSslProtocolSuites;

    // protocol
    private Boolean allowMultiQueries;
    private Boolean allowLocalInfile;
    private Boolean useCompression;
    private Boolean useAffectedRows;
    private Boolean disablePipeline;

    // prepare
    private Boolean cachePrepStmts;
    private Integer prepStmtCacheSize;
    private Boolean useServerPrepStmts;

    // authentication
    private String credentialType;
    private String sessionVariables;
    private String connectionAttributes;
    private String servicePrincipalName;
    private String jaasApplicationName;

    // meta
    private Boolean blankTableNameMeta;
    private Boolean tinyInt1isBit;

    private Boolean transformedBitIsBoolean;
    private Boolean yearIsDateType;
    private Boolean dumpQueriesOnException;
    private Boolean includeThreadDumpInDeadlockExceptions;

    // HA options
    private Integer retriesAllDown;
    private Boolean transactionReplay;
    private Integer transactionReplaySize;

    // Pool options
    private Boolean pool;
    private String poolName;
    private Integer maxPoolSize;
    private Integer minPoolSize;
    private Integer maxIdleTime;
    private Boolean registerJmxPool;
    private Integer poolValidMinDelay;
    private Boolean useResetConnection;

    private Boolean useMysqlVersion;

    private Boolean rewriteBatchedStatements;
    private String consoleLogLevel;
    private String consoleLogFilepath;
    private Boolean printStackTrace;
    private Integer maxPrintStackSizeToLog;

    public Builder user(String user) {
      this.user = nullOrEmpty(user);
      return this;
    }

    public Builder serverSslCert(String serverSslCert) {
      this.serverSslCert = nullOrEmpty(serverSslCert);
      return this;
    }

    public Builder trustStore(String trustStore) {
      this.trustStore = nullOrEmpty(trustStore);
      return this;
    }

    public Builder trustStorePassword(String trustStorePassword) {
      this.trustStorePassword = nullOrEmpty(trustStorePassword);
      return this;
    }

    public Builder trustStoreType(String trustStoreType) {
      this.trustStoreType = nullOrEmpty(trustStoreType);
      return this;
    }

    /**
     * File path of the keyStore file that contain client private key store and associate
     * certificates (similar to java System property \"javax.net.ssl.keyStore\", but ensure that
     * only the private key's entries are used)
     *
     * @param keyStore client store certificates
     * @return this {@link Builder}
     */
    public Builder keyStore(String keyStore) {
      this.keyStore = nullOrEmpty(keyStore);
      return this;
    }

    /**
     * Client keystore password
     *
     * @param keyStorePassword client store password
     * @return this {@link Builder}
     */
    public Builder keyStorePassword(String keyStorePassword) {
      this.keyStorePassword = nullOrEmpty(keyStorePassword);
      return this;
    }

    /**
     * Client keystore alias password
     *
     * @param keyPassword client store alias password
     * @return this {@link Builder}
     */
    public Builder keyPassword(String keyPassword) {
      this.keyPassword = nullOrEmpty(keyPassword);
      return this;
    }

    public Builder keyStoreType(String keyStoreType) {
      this.keyStoreType = nullOrEmpty(keyStoreType);
      return this;
    }

    public Builder password(String password) {
      this.password = nullOrEmpty(password);
      return this;
    }

    public Builder enabledSslProtocolSuites(String enabledSslProtocolSuites) {
      this.enabledSslProtocolSuites = nullOrEmpty(enabledSslProtocolSuites);
      return this;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder haMode(HaMode haMode) {
      this._haMode = haMode;
      return this;
    }

    public Builder addHost(String host, int port) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port));
      return this;
    }

    public Builder addHost(String host, int port, boolean master) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port, master));
      return this;
    }

    public Builder addresses(HostAddress... hostAddress) {
      this._addresses = new ArrayList<>();
      this._addresses.addAll(Arrays.asList(hostAddress));
      return this;
    }

    public Builder socketFactory(String socketFactory) {
      this.socketFactory = socketFactory;
      return this;
    }

    /**
     * Indicate connect timeout value, in milliseconds, or zero for no timeout. Default: 30000
     *
     * @param connectTimeout connect Timeout
     * @return this {@link Builder}
     */
    public Builder connectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Indicate to use windows named pipe, specify named pipe name to connect
     *
     * @param pipe windows named pipe
     * @return this {@link Builder}
     */
    public Builder pipe(String pipe) {
      this.pipe = nullOrEmpty(pipe);
      return this;
    }

    /**
     * Indicate to use Unix domain socket, if the server allows it, specifying named pipe name to
     * connect The value is the path of Unix domain socket (available with "select @@socket"
     * command).
     *
     * @param localSocket local socket path
     * @return this {@link Builder}
     */
    public Builder localSocket(String localSocket) {
      this.localSocket = nullOrEmpty(localSocket);
      return this;
    }

    /**
     * Indicate if TCP keep-alive must be enable.
     *
     * @param tcpKeepAlive value
     * @return this {@link Builder}
     */
    public Builder tcpKeepAlive(Boolean tcpKeepAlive) {
      this.tcpKeepAlive = tcpKeepAlive;
      return this;
    }

    /**
     * Indicate TCP keep-idle value (for java 11+ only).
     *
     * @param tcpKeepIdle value
     * @return this {@link Builder}
     */
    public Builder tcpKeepIdle(Integer tcpKeepIdle) {
      this.tcpKeepIdle = tcpKeepIdle;
      return this;
    }

    /**
     * Indicate TCP keep-count value (for java 11+ only).
     *
     * @param tcpKeepCount value
     * @return this {@link Builder}
     */
    public Builder tcpKeepCount(Integer tcpKeepCount) {
      this.tcpKeepCount = tcpKeepCount;
      return this;
    }

    /**
     * Indicate TCP keep-interval value (for java 11+ only).
     *
     * @param tcpKeepInterval value
     * @return this {@link Builder}
     */
    public Builder tcpKeepInterval(Integer tcpKeepInterval) {
      this.tcpKeepInterval = tcpKeepInterval;
      return this;
    }

    /**
     * Indicate that when connection fails, to send an RST TCP packet.
     *
     * @param tcpAbortiveClose value
     * @return this {@link Builder}
     */
    public Builder tcpAbortiveClose(Boolean tcpAbortiveClose) {
      this.tcpAbortiveClose = tcpAbortiveClose;
      return this;
    }

    /**
     * Indicate what default Object type Geometry a resultset.getObject must return. possibility :
     *
     * <ul>
     *   <li>null or empty is WKB byte array
     *   <li>'default' will return com.singlestore.jdbc.type Object
     * </ul>
     *
     * In the future JTS might be implemented
     *
     * @param geometryDefault value
     * @return this {@link Builder}
     */
    public Builder geometryDefaultType(String geometryDefault) {
      this.geometryDefaultType = nullOrEmpty(geometryDefault);
      return this;
    }

    /**
     * restrict authentication method to secure list. Default "default".
     *
     * @param restrictedAuth use authentication plugin list
     * @return this {@link Builder}
     */
    public Builder restrictedAuth(String restrictedAuth) {
      this.restrictedAuth = restrictedAuth;
      return this;
    }

    /**
     * Indicate Hostname or IP address to bind the connection socket to a local (UNIX domain)
     * socket.
     *
     * @param localSocketAddress Hostname or IP address
     * @return this {@link Builder}
     */
    public Builder localSocketAddress(String localSocketAddress) {
      this.localSocketAddress = nullOrEmpty(localSocketAddress);
      return this;
    }

    /**
     * Indicate the network socket timeout (SO_TIMEOUT) in milliseconds. Value of 0 disables this
     * timeout.
     *
     * <p>If the goal is to set a timeout for all queries, the server has permitted a solution to
     * limit the query time by setting a system variable, max_statement_time. Default: 0
     *
     * @param socketTimeout socket timeout value
     * @return this {@link Builder}
     */
    public Builder socketTimeout(Integer socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    /**
     * Indicate that multi-queries are allowed. example: "insert into ab (i) values (1); insert into
     * ab (i) values (2)".
     *
     * <p>If application build sql command string, this is probably a bad idea to enable this
     * option, opening the door to sql injection. default: false.
     *
     * @param allowMultiQueries indicate if active
     * @return this {@link Builder}
     */
    public Builder allowMultiQueries(Boolean allowMultiQueries) {
      this.allowMultiQueries = allowMultiQueries;
      return this;
    }

    /**
     * Indicate if LOAD DATA LOCAL INFILE are permitted. This will disable all pipelining
     * implementation.
     *
     * @param allowLocalInfile indicate if permit LOAD DATA LOCAL INFILE commands
     * @return this {@link Builder}
     */
    public Builder allowLocalInfile(Boolean allowLocalInfile) {
      this.allowLocalInfile = allowLocalInfile;
      return this;
    }

    /**
     * Indicate to compresses exchanges with the database through gzip. This permits better
     * performance when the database is not in the same location.
     *
     * @param useCompression to enable/disable compression
     * @return this {@link Builder}
     */
    public Builder useCompression(Boolean useCompression) {
      this.useCompression = useCompression;
      return this;
    }

    public Builder blankTableNameMeta(Boolean blankTableNameMeta) {
      this.blankTableNameMeta = blankTableNameMeta;
      return this;
    }

    public Builder credentialType(String credentialType) {
      this.credentialType = nullOrEmpty(credentialType);
      return this;
    }

    public Builder sslMode(String sslMode) {
      this.sslMode = sslMode;
      return this;
    }

    public Builder transactionIsolation(String transactionIsolation) {
      this.transactionIsolation = nullOrEmpty(transactionIsolation);
      return this;
    }

    public Builder enabledSslCipherSuites(String enabledSslCipherSuites) {
      this.enabledSslCipherSuites = nullOrEmpty(enabledSslCipherSuites);
      return this;
    }

    public Builder sessionVariables(String sessionVariables) {
      this.sessionVariables = nullOrEmpty(sessionVariables);
      return this;
    }

    /**
     * TinyInt(1) to be considered as bit
     *
     * @param tinyInt1isBit Indicate if Tinyint(1) to be considered as bit
     * @return this {@link Builder}
     */
    public Builder tinyInt1isBit(Boolean tinyInt1isBit) {
      this.tinyInt1isBit = tinyInt1isBit;
      return this;
    }

    /**
     * TinyInt(1) to be considered as boolean
     *
     * @param transformedBitIsBoolean Indicate if Tinyint(1) to be considered as boolean
     * @return this {@link Builder}
     */
    public Builder transformedBitIsBoolean(Boolean transformedBitIsBoolean) {
      this.transformedBitIsBoolean = transformedBitIsBoolean;
      return this;
    }

    public Builder yearIsDateType(Boolean yearIsDateType) {
      this.yearIsDateType = yearIsDateType;
      return this;
    }

    public Builder dumpQueriesOnException(Boolean dumpQueriesOnException) {
      this.dumpQueriesOnException = dumpQueriesOnException;
      return this;
    }

    public Builder prepStmtCacheSize(Integer prepStmtCacheSize) {
      this.prepStmtCacheSize = prepStmtCacheSize;
      return this;
    }

    public Builder useAffectedRows(Boolean useAffectedRows) {
      this.useAffectedRows = useAffectedRows;
      return this;
    }

    public Builder disablePipeline(Boolean disablePipeline) {
      this.disablePipeline = disablePipeline;
      return this;
    }

    public Builder useServerPrepStmts(Boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    /**
     * Permit to force autocommit connection value
     *
     * @param autocommit autocommit value
     * @return this {@link Builder}
     */
    public Builder autocommit(Boolean autocommit) {
      this.autocommit = autocommit;
      return this;
    }

    /**
     * Create database if not exist. This is mainly for test, since does require an additional query
     * after connection
     *
     * @param createDatabaseIfNotExist must driver create database if doesn't exist
     * @return this {@link Builder}
     */
    public Builder createDatabaseIfNotExist(Boolean createDatabaseIfNotExist) {
      this.createDatabaseIfNotExist = createDatabaseIfNotExist;
      return this;
    }

    /**
     * permit to execute an SQL command on connection creation
     *
     * @param initSql initial SQL command
     * @return this {@link Builder}
     */
    public Builder initSql(String initSql) {
      this.initSql = initSql;
      return this;
    }

    public Builder connectionAttributes(String connectionAttributes) {
      this.connectionAttributes = nullOrEmpty(connectionAttributes);
      return this;
    }

    public Builder includeThreadDumpInDeadlockExceptions(
        Boolean includeThreadDumpInDeadlockExceptions) {
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
      return this;
    }

    public Builder servicePrincipalName(String servicePrincipalName) {
      this.servicePrincipalName = nullOrEmpty(servicePrincipalName);
      return this;
    }

    public Builder jaasApplicationName(String jaasApplicationName) {
      this.jaasApplicationName = nullOrEmpty(jaasApplicationName);
      return this;
    }

    public Builder defaultFetchSize(Integer defaultFetchSize) {
      this.defaultFetchSize = defaultFetchSize;
      return this;
    }

    public Builder tlsSocketType(String tlsSocketType) {
      this.tlsSocketType = nullOrEmpty(tlsSocketType);
      return this;
    }

    public Builder maxQuerySizeToLog(Integer maxQuerySizeToLog) {
      this.maxQuerySizeToLog = maxQuerySizeToLog;
      return this;
    }

    /**
     * Indicate to driver server max_allowed_packet. This permit to driver to avoid sending commands
     * too big, that would have make server to drop connection
     *
     * @param maxAllowedPacket indicate server max_allowed_packet value
     * @return this {@link Builder}
     */
    public Builder maxAllowedPacket(Integer maxAllowedPacket) {
      this.maxAllowedPacket = maxAllowedPacket;
      return this;
    }

    public Builder retriesAllDown(Integer retriesAllDown) {
      this.retriesAllDown = retriesAllDown;
      return this;
    }

    public Builder pool(Boolean pool) {
      this.pool = pool;
      return this;
    }

    public Builder poolName(String poolName) {
      this.poolName = nullOrEmpty(poolName);
      return this;
    }

    public Builder maxPoolSize(Integer maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    public Builder minPoolSize(Integer minPoolSize) {
      this.minPoolSize = minPoolSize;
      return this;
    }

    public Builder maxIdleTime(Integer maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
      return this;
    }

    public Builder registerJmxPool(Boolean registerJmxPool) {
      this.registerJmxPool = registerJmxPool;
      return this;
    }

    public Builder poolValidMinDelay(Integer poolValidMinDelay) {
      this.poolValidMinDelay = poolValidMinDelay;
      return this;
    }

    public Builder useResetConnection(Boolean useResetConnection) {
      this.useResetConnection = useResetConnection;
      return this;
    }

    /**
     * Cache all socket available information.
     *
     * @param useReadAheadInput cache available socket data when reading socket.
     * @return this {@link Builder}
     */
    public Builder useReadAheadInput(Boolean useReadAheadInput) {
      this.useReadAheadInput = useReadAheadInput;
      return this;
    }

    /**
     * Cache server prepare result
     *
     * @param cachePrepStmts cache server prepared result
     * @return this {@link Builder}
     */
    public Builder cachePrepStmts(Boolean cachePrepStmts) {
      this.cachePrepStmts = cachePrepStmts;
      return this;
    }

    /**
     * Must cache commands in transaction and replay transaction on failover.
     *
     * @param transactionReplay cache transaction and replay on failover
     * @return this {@link Builder}
     */
    public Builder transactionReplay(Boolean transactionReplay) {
      this.transactionReplay = transactionReplay;
      return this;
    }

    /**
     * Transaction replay cache size
     *
     * @param transactionReplaySize transaction replay cache size
     * @return this {@link Builder}
     */
    public Builder transactionReplaySize(Integer transactionReplaySize) {
      this.transactionReplaySize = transactionReplaySize;
      return this;
    }

    public Builder useMysqlVersion(Boolean useMysqlVersion) {
      this.useMysqlVersion = useMysqlVersion;
      return this;
    }

    public Builder rewriteBatchedStatements(Boolean rewriteBatchedStatements) {
      this.rewriteBatchedStatements = rewriteBatchedStatements;
      return this;
    }

    public Builder consoleLogLevel(String consoleLogLevel) {
      this.consoleLogLevel = consoleLogLevel;
      return this;
    }

    public Builder consoleLogFilepath(String consoleLogFilepath) {
      this.consoleLogFilepath = consoleLogFilepath;
      return this;
    }

    public Builder printStackTrace(Boolean printStackTrace) {
      this.printStackTrace = printStackTrace;
      return this;
    }

    public Builder maxPrintStackSizeToLog(Integer maxPrintStackSizeToLog) {
      this.maxPrintStackSizeToLog = maxPrintStackSizeToLog;
      return this;
    }

    /**
     * Build a configuration
     *
     * @return a Configuration object
     * @throws SQLException if option data type doesn't correspond
     */
    public Configuration build() throws SQLException {
      Configuration conf =
          new Configuration(
              this.database,
              this._addresses,
              this._haMode,
              this.user,
              this.password,
              this.enabledSslProtocolSuites,
              this.socketFactory,
              this.connectTimeout,
              this.pipe,
              this.localSocket,
              this.tcpKeepAlive,
              this.tcpKeepIdle,
              this.tcpKeepCount,
              this.tcpKeepInterval,
              this.tcpAbortiveClose,
              this.localSocketAddress,
              this.socketTimeout,
              this.allowMultiQueries,
              this.allowLocalInfile,
              this.useCompression,
              this.blankTableNameMeta,
              this.credentialType,
              this.sslMode,
              this.transactionIsolation,
              this.enabledSslCipherSuites,
              this.sessionVariables,
              this.tinyInt1isBit,
              this.transformedBitIsBoolean,
              this.yearIsDateType,
              this.dumpQueriesOnException,
              this.prepStmtCacheSize,
              this.useAffectedRows,
              this.disablePipeline,
              this.useServerPrepStmts,
              this.connectionAttributes,
              this.autocommit,
              this.createDatabaseIfNotExist,
              this.initSql,
              this.includeThreadDumpInDeadlockExceptions,
              this.servicePrincipalName,
              this.jaasApplicationName,
              this.defaultFetchSize,
              this.tlsSocketType,
              this.maxQuerySizeToLog,
              this.maxAllowedPacket,
              this.retriesAllDown,
              this.pool,
              this.poolName,
              this.maxPoolSize,
              this.minPoolSize,
              this.maxIdleTime,
              this.registerJmxPool,
              this.poolValidMinDelay,
              this.useResetConnection,
              this.serverSslCert,
              this.trustStore,
              this.trustStorePassword,
              this.trustStoreType,
              this.keyStore,
              this.keyStorePassword,
              this.keyPassword,
              this.keyStoreType,
              this.useReadAheadInput,
              this.cachePrepStmts,
              this.transactionReplay,
              this.transactionReplaySize,
              this.geometryDefaultType,
              this.restrictedAuth,
              this._nonMappedOptions,
              this.useMysqlVersion,
              this.rewriteBatchedStatements,
              this.consoleLogLevel,
              this.consoleLogFilepath,
              this.printStackTrace,
              this.maxPrintStackSizeToLog);
      conf.initialUrl = buildUrl(conf);
      return conf;
    }
  }

  private static String nullOrEmpty(String val) {
    return (val == null || val.isEmpty()) ? null : val;
  }
}
