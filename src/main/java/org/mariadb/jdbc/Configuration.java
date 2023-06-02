// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.CredentialPlugin;
import org.mariadb.jdbc.plugin.credential.CredentialPluginLoader;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;
import org.mariadb.jdbc.util.options.OptionAliases;

/**
 * parse and verification of URL.
 *
 * <p>basic syntax :<br>
 * {@code
 * jdbc:mariadb:[replication:|failover|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>]/[database>]
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
 * exemple : {@code jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 *
 * <p>Some examples :<br>
 * {@code jdbc:mariadb://localhost:3306/database?user=greg&password=pass}<br>
 * {@code
 * jdbc:mariadb://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}
 * <br>
 */
public class Configuration {
  private static final Logger logger = Loggers.getLogger(Configuration.class);

  // standard options
  private String user = null;
  private String password = null;
  private String database = null;
  private List<HostAddress> addresses = null;
  private HaMode haMode = HaMode.NONE;

  private String initialUrl = null;
  private Properties nonMappedOptions = null;

  // various
  private String timezone = null;
  private Boolean autocommit = null;
  private boolean useMysqlMetadata = false;
  private boolean createDatabaseIfNotExist = false;
  private TransactionIsolation transactionIsolation = null;
  private int defaultFetchSize = 0;
  private int maxQuerySizeToLog = 1024;
  private Integer maxAllowedPacket = null;
  private String geometryDefaultType = null;
  private String restrictedAuth = null;
  private String initSql = null;

  // socket
  private String socketFactory = null;
  private int connectTimeout =
      DriverManager.getLoginTimeout() > 0 ? DriverManager.getLoginTimeout() * 1000 : 30_000;
  private String pipe = null;
  private String localSocket = null;
  private boolean uuidAsString = false;
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
  private String keyStore = null;
  private String keyStorePassword = null;
  private String keyStoreType = null;
  private String trustStoreType = null;
  private String enabledSslCipherSuites = null;
  private String enabledSslProtocolSuites = null;

  // protocol
  private boolean allowMultiQueries = false;
  private boolean allowLocalInfile = true;
  private boolean useCompression = false;
  private boolean useAffectedRows = false;
  private boolean useBulkStmts = true;
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

  // meta
  private boolean blankTableNameMeta = false;
  private boolean tinyInt1isBit = true;
  private boolean transformedBitIsBoolean = true;
  private boolean yearIsDateType = true;
  private boolean dumpQueriesOnException = false;
  private boolean includeInnodbStatusInDeadlockExceptions = false;
  private boolean includeThreadDumpInDeadlockExceptions = false;

  // HA options
  private int retriesAllDown = 120;
  private String galeraAllowedState = null;
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

  // MySQL sha authentication
  private String serverRsaPublicKeyFile = null;
  private boolean allowPublicKeyRetrieval = false;

  private Codec<?>[] codecs = null;

  private Configuration() {}

  private Configuration(
      String user,
      String password,
      String database,
      List<HostAddress> addresses,
      HaMode haMode,
      Properties nonMappedOptions,
      String timezone,
      Boolean autocommit,
      boolean useMysqlMetadata,
      boolean createDatabaseIfNotExist,
      TransactionIsolation transactionIsolation,
      int defaultFetchSize,
      int maxQuerySizeToLog,
      Integer maxAllowedPacket,
      String geometryDefaultType,
      String restrictedAuth,
      String initSql,
      String socketFactory,
      int connectTimeout,
      String pipe,
      String localSocket,
      boolean tcpKeepAlive,
      boolean uuidAsString,
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
      String keyStore,
      String keyStorePassword,
      String keyStoreType,
      String trustStoreType,
      String enabledSslCipherSuites,
      String enabledSslProtocolSuites,
      boolean allowMultiQueries,
      boolean allowLocalInfile,
      boolean useCompression,
      boolean useAffectedRows,
      boolean useBulkStmts,
      boolean disablePipeline,
      boolean cachePrepStmts,
      int prepStmtCacheSize,
      boolean useServerPrepStmts,
      CredentialPlugin credentialType,
      String sessionVariables,
      String connectionAttributes,
      String servicePrincipalName,
      boolean blankTableNameMeta,
      boolean tinyInt1isBit,
      boolean transformedBitIsBoolean,
      boolean yearIsDateType,
      boolean dumpQueriesOnException,
      boolean includeInnodbStatusInDeadlockExceptions,
      boolean includeThreadDumpInDeadlockExceptions,
      int retriesAllDown,
      String galeraAllowedState,
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
      String serverRsaPublicKeyFile,
      boolean allowPublicKeyRetrieval) {
    this.user = user;
    this.password = password;
    this.database = database;
    this.addresses = addresses;
    this.haMode = haMode;
    this.nonMappedOptions = nonMappedOptions;
    this.timezone = timezone;
    this.autocommit = autocommit;
    this.useMysqlMetadata = useMysqlMetadata;
    this.createDatabaseIfNotExist = createDatabaseIfNotExist;
    this.transactionIsolation = transactionIsolation;
    this.defaultFetchSize = defaultFetchSize;
    this.maxQuerySizeToLog = maxQuerySizeToLog;
    this.maxAllowedPacket = maxAllowedPacket;
    this.geometryDefaultType = geometryDefaultType;
    this.restrictedAuth = restrictedAuth;
    this.initSql = initSql;
    this.socketFactory = socketFactory;
    this.connectTimeout = connectTimeout;
    this.pipe = pipe;
    this.localSocket = localSocket;
    this.tcpKeepAlive = tcpKeepAlive;
    this.uuidAsString = uuidAsString;
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
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.keyStoreType = keyStoreType;
    this.trustStoreType = trustStoreType;
    this.enabledSslCipherSuites = enabledSslCipherSuites;
    this.enabledSslProtocolSuites = enabledSslProtocolSuites;
    this.allowMultiQueries = allowMultiQueries;
    this.allowLocalInfile = allowLocalInfile;
    this.useCompression = useCompression;
    this.useAffectedRows = useAffectedRows;
    this.useBulkStmts = useBulkStmts;
    this.disablePipeline = disablePipeline;
    this.cachePrepStmts = cachePrepStmts;
    this.prepStmtCacheSize = prepStmtCacheSize;
    this.useServerPrepStmts = useServerPrepStmts;
    this.credentialType = credentialType;
    this.sessionVariables = sessionVariables;
    this.connectionAttributes = connectionAttributes;
    this.servicePrincipalName = servicePrincipalName;
    this.blankTableNameMeta = blankTableNameMeta;
    this.tinyInt1isBit = tinyInt1isBit;
    this.transformedBitIsBoolean = transformedBitIsBoolean;
    this.yearIsDateType = yearIsDateType;
    this.dumpQueriesOnException = dumpQueriesOnException;
    this.includeInnodbStatusInDeadlockExceptions = includeInnodbStatusInDeadlockExceptions;
    this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
    this.retriesAllDown = retriesAllDown;
    this.galeraAllowedState = galeraAllowedState;
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
    this.serverRsaPublicKeyFile = serverRsaPublicKeyFile;
    this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
    this.initialUrl = buildUrl(this);
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
      Boolean uuidAsString,
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
      String timezone,
      Boolean dumpQueriesOnException,
      Integer prepStmtCacheSize,
      Boolean useAffectedRows,
      Boolean useServerPrepStmts,
      String connectionAttributes,
      Boolean useBulkStmts,
      Boolean disablePipeline,
      Boolean autocommit,
      Boolean useMysqlMetadata,
      Boolean createDatabaseIfNotExist,
      Boolean includeInnodbStatusInDeadlockExceptions,
      Boolean includeThreadDumpInDeadlockExceptions,
      String servicePrincipalName,
      Integer defaultFetchSize,
      String tlsSocketType,
      Integer maxQuerySizeToLog,
      Integer maxAllowedPacket,
      Integer retriesAllDown,
      String galeraAllowedState,
      Boolean pool,
      String poolName,
      Integer maxPoolSize,
      Integer minPoolSize,
      Integer maxIdleTime,
      Boolean registerJmxPool,
      Integer poolValidMinDelay,
      Boolean useResetConnection,
      String serverRsaPublicKeyFile,
      Boolean allowPublicKeyRetrieval,
      String serverSslCert,
      String keyStore,
      String keyStorePassword,
      String keyStoreType,
      String trustStoreType,
      Boolean useReadAheadInput,
      Boolean cachePrepStmts,
      Boolean transactionReplay,
      Integer transactionReplaySize,
      String geometryDefaultType,
      String restrictedAuth,
      String initSql,
      Properties nonMappedOptions)
      throws SQLException {
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
    if (uuidAsString != null) this.uuidAsString = uuidAsString;
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
    this.timezone = timezone;
    if (dumpQueriesOnException != null) this.dumpQueriesOnException = dumpQueriesOnException;
    if (prepStmtCacheSize != null) this.prepStmtCacheSize = prepStmtCacheSize;
    if (useAffectedRows != null) this.useAffectedRows = useAffectedRows;
    if (useServerPrepStmts != null) this.useServerPrepStmts = useServerPrepStmts;
    this.connectionAttributes = connectionAttributes;
    if (useBulkStmts != null) this.useBulkStmts = useBulkStmts;
    if (disablePipeline != null) this.disablePipeline = disablePipeline;
    if (autocommit != null) this.autocommit = autocommit;
    if (useMysqlMetadata != null) this.useMysqlMetadata = useMysqlMetadata;
    if (createDatabaseIfNotExist != null) this.createDatabaseIfNotExist = createDatabaseIfNotExist;
    if (includeInnodbStatusInDeadlockExceptions != null)
      this.includeInnodbStatusInDeadlockExceptions = includeInnodbStatusInDeadlockExceptions;
    if (includeThreadDumpInDeadlockExceptions != null)
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
    if (servicePrincipalName != null) this.servicePrincipalName = servicePrincipalName;
    if (defaultFetchSize != null) this.defaultFetchSize = defaultFetchSize;
    if (tlsSocketType != null) this.tlsSocketType = tlsSocketType;
    if (maxQuerySizeToLog != null) this.maxQuerySizeToLog = maxQuerySizeToLog;
    if (maxAllowedPacket != null) this.maxAllowedPacket = maxAllowedPacket;
    if (retriesAllDown != null) this.retriesAllDown = retriesAllDown;
    if (galeraAllowedState != null) this.galeraAllowedState = galeraAllowedState;
    if (pool != null) this.pool = pool;
    if (poolName != null) this.poolName = poolName;
    if (maxPoolSize != null) this.maxPoolSize = maxPoolSize;
    // if min pool size default to maximum pool size if not set
    if (minPoolSize != null) {
      this.minPoolSize = minPoolSize;
    } else {
      this.minPoolSize = this.maxPoolSize;
    }

    if (maxIdleTime != null) this.maxIdleTime = maxIdleTime;
    if (registerJmxPool != null) this.registerJmxPool = registerJmxPool;
    if (poolValidMinDelay != null) this.poolValidMinDelay = poolValidMinDelay;
    if (useResetConnection != null) this.useResetConnection = useResetConnection;
    if (serverRsaPublicKeyFile != null) this.serverRsaPublicKeyFile = serverRsaPublicKeyFile;
    if (allowPublicKeyRetrieval != null) this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
    if (useReadAheadInput != null) this.useReadAheadInput = useReadAheadInput;
    if (cachePrepStmts != null) this.cachePrepStmts = cachePrepStmts;
    if (transactionReplay != null) this.transactionReplay = transactionReplay;
    if (transactionReplaySize != null) this.transactionReplaySize = transactionReplaySize;
    if (geometryDefaultType != null) this.geometryDefaultType = geometryDefaultType;
    if (restrictedAuth != null) this.restrictedAuth = restrictedAuth;
    if (initSql != null) this.initSql = initSql;
    if (serverSslCert != null) this.serverSslCert = serverSslCert;
    if (keyStore != null) this.keyStore = keyStore;
    if (keyStorePassword != null) this.keyStorePassword = keyStorePassword;
    if (keyStoreType != null) this.keyStoreType = keyStoreType;
    if (trustStoreType != null) this.trustStoreType = trustStoreType;

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
   * Tell if mariadb driver accept url string. (Correspond to interface
   * java.jdbc.Driver.acceptsURL() method)
   *
   * @param url url String
   * @return true if url string correspond.
   */
  public static boolean acceptsUrl(String url) {
    return url != null
        && (url.startsWith("jdbc:mariadb:")
            || (url.startsWith("jdbc:mysql:") && url.contains("permitMysqlScheme")));
  }

  /**
   * parse connection string
   *
   * @param url connection string
   * @return configuration resulting object
   * @throws SQLException if not supported driver or wrong connection string format.
   */
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
        Properties deprecatedDesc = new Properties();
        try (InputStream inputStream =
            Driver.class.getClassLoader().getResourceAsStream("deprecated.properties")) {
          deprecatedDesc.load(inputStream);
        } catch (IOException io) {
          // eat
        }
        logger.warn(deprecatedDesc.getProperty("useSsl"));
        if (isSet("trustServerCertificate", nonMappedOptions)) {
          builder.sslMode("trust");
          logger.warn(deprecatedDesc.getProperty("trustServerCertificate"));
        } else if (isSet("disableSslHostnameVerification", nonMappedOptions)) {
          logger.warn(deprecatedDesc.getProperty("disableSslHostnameVerification"));
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
    // parser is sure to have at least 2 colon, since jdbc:[mysql|mariadb]: is tested.
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
   * Clone configuration with another user/password
   *
   * @param username new username
   * @param password new password
   * @return new cloned configuration object
   */
  public Configuration clone(String username, String password) {
    return new Configuration(
        username != null && username.isEmpty() ? null : username,
        password != null && password.isEmpty() ? null : password,
        this.database,
        this.addresses,
        this.haMode,
        this.nonMappedOptions,
        this.timezone,
        this.autocommit,
        this.useMysqlMetadata,
        this.createDatabaseIfNotExist,
        this.transactionIsolation,
        this.defaultFetchSize,
        this.maxQuerySizeToLog,
        this.maxAllowedPacket,
        this.geometryDefaultType,
        this.restrictedAuth,
        this.initSql,
        this.socketFactory,
        this.connectTimeout,
        this.pipe,
        this.localSocket,
        this.tcpKeepAlive,
        this.uuidAsString,
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
        this.keyStore,
        this.keyStorePassword,
        this.keyStoreType,
        this.trustStoreType,
        this.enabledSslCipherSuites,
        this.enabledSslProtocolSuites,
        this.allowMultiQueries,
        this.allowLocalInfile,
        this.useCompression,
        this.useAffectedRows,
        this.useBulkStmts,
        this.disablePipeline,
        this.cachePrepStmts,
        this.prepStmtCacheSize,
        this.useServerPrepStmts,
        this.credentialType,
        this.sessionVariables,
        this.connectionAttributes,
        this.servicePrincipalName,
        this.blankTableNameMeta,
        this.tinyInt1isBit,
        this.transformedBitIsBoolean,
        this.yearIsDateType,
        this.dumpQueriesOnException,
        this.includeInnodbStatusInDeadlockExceptions,
        this.includeThreadDumpInDeadlockExceptions,
        this.retriesAllDown,
        this.galeraAllowedState,
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
        this.serverRsaPublicKeyFile,
        this.allowPublicKeyRetrieval);
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
   * key store type (to replace default javax.net.ssl.keyStoreType system property)
   *
   * @return key store type
   */
  public String keyStoreType() {
    return keyStoreType;
  }

  /**
   * trust store type (to replace default javax.net.ssl.keyStoreType system property)
   *
   * @return trust store type
   */
  public String trustStoreType() {
    return trustStoreType;
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
   * must uuid fields return as String and not java.util.UUID
   *
   * @return must UUID return as String and not uuid
   */
  public boolean uuidAsString() {
    return uuidAsString;
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
   * Must tinyint(1) be considered as Bit
   *
   * @return true if tinyint(1) must be considered as Bit
   */
  public boolean tinyInt1isBit() {
    return tinyInt1isBit;
  }

  /**
   * Must tinyint(1) be considered as Boolean or Bit
   *
   * @return true if tinyint(1) must be considered as Boolean
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
   * Set timezone
   *
   * @return timezone
   */
  public String timezone() {
    return timezone;
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

  /**
   * Use server prepared statement. IF false, using client prepared statement.
   *
   * @return use server prepared statement
   */
  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  /**
   * Connections attributes
   *
   * @return connection meta informations
   */
  public String connectionAttributes() {
    return connectionAttributes;
  }

  /**
   * Use server COM_STMT_BULK for batching.
   *
   * @return use server bulk command.
   */
  public boolean useBulkStmts() {
    return useBulkStmts;
  }

  /**
   * Disable pipeline.
   *
   * @return is pipeline disabled.
   */
  public boolean disablePipeline() {
    return disablePipeline;
  }

  /**
   * Force session autocommit on connection creation
   *
   * @return autocommit forced value
   */
  public Boolean autocommit() {
    return autocommit;
  }

  /**
   * Force returning MySQL metadata information
   *
   * @return force returning MySQL in metadata
   */
  public boolean useMysqlMetadata() {
    return useMysqlMetadata;
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
   * On deadlock exception, must driver execute additional commands to show innodb status in error
   * description.
   *
   * @return includeInnodbStatusInDeadlockExceptions
   */
  public boolean includeInnodbStatusInDeadlockExceptions() {
    return includeInnodbStatusInDeadlockExceptions;
  }

  /**
   * On deadlock exception, must driver display threads information on error description.
   *
   * @return include Thread Dump In Deadlock Exceptions
   */
  public boolean includeThreadDumpInDeadlockExceptions() {
    return includeThreadDumpInDeadlockExceptions;
  }

  /**
   * Service principal name (GSSAPI option)
   *
   * @return service principal name
   */
  public String servicePrincipalName() {
    return servicePrincipalName;
  }

  /**
   * result-set streaming default fetch size
   *
   * @return Default fetch size.
   */
  public int defaultFetchSize() {
    return defaultFetchSize;
  }

  /**
   * non standard options
   *
   * @return non standard options
   */
  public Properties nonMappedOptions() {
    return nonMappedOptions;
  }

  /**
   * TLS socket type
   *
   * @return TLS socket type
   */
  public String tlsSocketType() {
    return tlsSocketType;
  }

  /**
   * query maximum size to log (query will be truncated of more than this limit)
   *
   * @return max query log size
   */
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

  /**
   * retry the maximum retry number of attempts to reconnect after a failover.
   *
   * @return the maximum retry number of attempts to reconnect after a failover.
   */
  public int retriesAllDown() {
    return retriesAllDown;
  }

  /**
   * Galera comma separated allowed state
   *
   * @return galera allowed state
   */
  public String galeraAllowedState() {
    return galeraAllowedState;
  }

  /**
   * Create pool
   *
   * @return create pool if don't exists
   */
  public boolean pool() {
    return pool;
  }

  /**
   * pool name
   *
   * @return pool name.
   */
  public String poolName() {
    return poolName;
  }

  /**
   * max pool size
   *
   * @return maximum pool size
   */
  public int maxPoolSize() {
    return maxPoolSize;
  }

  /**
   * Minimum pool size
   *
   * @return minimum pool size
   */
  public int minPoolSize() {
    return minPoolSize;
  }

  /**
   * Max idle time
   *
   * @return pool max idle time.
   */
  public int maxIdleTime() {
    return maxIdleTime;
  }

  /**
   * register pool information to JMX
   *
   * @return register pool to JMX
   */
  public boolean registerJmxPool() {
    return registerJmxPool;
  }

  /**
   * Pool mininum validation delay.
   *
   * @return pool validation delay
   */
  public int poolValidMinDelay() {
    return poolValidMinDelay;
  }

  /**
   * Must connection returned to pool be RESET
   *
   * @return use RESET on connection
   */
  public boolean useResetConnection() {
    return useResetConnection;
  }

  /**
   * Server RSA public key file for caching_sha2_password authentication
   *
   * @return server key file
   */
  public String serverRsaPublicKeyFile() {
    return serverRsaPublicKeyFile;
  }

  /**
   * permit mysql authentication to retrieve server certificate
   *
   * @return is driver allowed to retrieve server certificate from server
   */
  public boolean allowPublicKeyRetrieval() {
    return allowPublicKeyRetrieval;
  }

  /**
   * Read all data from socket in advance
   *
   * @return use read ahead buffer implementation
   */
  public boolean useReadAheadInput() {
    return useReadAheadInput;
  }

  /**
   * Cache prepared statement result.
   *
   * @return cache prepare results
   */
  public boolean cachePrepStmts() {
    return cachePrepStmts;
  }

  /**
   * implements transaction replay failover
   *
   * @return true if transaction must be replayed on failover.
   */
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

  /**
   * geometry default decoding implementation
   *
   * @return geometry default type
   */
  public String geometryDefaultType() {
    return geometryDefaultType;
  }

  /**
   * Restrict authentication plugin to comma separated plugin list
   *
   * @return authorized authentication list
   */
  public String restrictedAuth() {
    return restrictedAuth;
  }

  /**
   * Execute initial command when connection is established
   *
   * @return initial SQL command
   */
  public String initSql() {
    return initSql;
  }

  /**
   * datatype Encoder/decoder list
   *
   * @return codec list
   */
  public Codec<?>[] codecs() {
    return codecs;
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

    if (password == null) {
      return initialUrl.equals(that.initialUrl) && that.password == null;
    }
    return initialUrl.equals(that.initialUrl) && password.equals(that.password);
  }

  /**
   * Generate initialURL property
   *
   * @param conf current configuration
   * @return initialUrl value.
   */
  protected static String buildUrl(Configuration conf) {
    Configuration defaultConf = new Configuration();
    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:mariadb:");
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
        if (hostAddress.port != 3306) sb.append(":").append(hostAddress.port);
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
              sb.append(obj);
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
    private String timezone;
    private Boolean autocommit;
    private Boolean useMysqlMetadata;
    private Boolean createDatabaseIfNotExist;
    private Integer defaultFetchSize;
    private Integer maxQuerySizeToLog;
    private Integer maxAllowedPacket;
    private String geometryDefaultType;
    private String restrictedAuth;
    private String initSql;
    private String transactionIsolation;

    // socket
    private String socketFactory;
    private Integer connectTimeout;
    private String pipe;
    private String localSocket;
    private Boolean tcpKeepAlive;
    private Boolean uuidAsString;
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
    private String keyStore;
    private String keyStorePassword;
    private String keyStoreType;
    private String trustStoreType;
    private String enabledSslCipherSuites;
    private String enabledSslProtocolSuites;

    // protocol
    private Boolean allowMultiQueries;
    private Boolean allowLocalInfile;
    private Boolean useCompression;
    private Boolean useAffectedRows;
    private Boolean useBulkStmts;
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

    // meta
    private Boolean blankTableNameMeta;
    private Boolean tinyInt1isBit;

    private Boolean transformedBitIsBoolean;
    private Boolean yearIsDateType;
    private Boolean dumpQueriesOnException;
    private Boolean includeInnodbStatusInDeadlockExceptions;
    private Boolean includeThreadDumpInDeadlockExceptions;

    // HA options
    private Integer retriesAllDown;
    private String galeraAllowedState;
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

    // MySQL sha authentication
    private String serverRsaPublicKeyFile;
    private Boolean allowPublicKeyRetrieval;

    /**
     * set user to authenticate to server
     *
     * @param user user
     * @return this {@link Builder}
     */
    public Builder user(String user) {
      this.user = nullOrEmpty(user);
      return this;
    }

    /**
     * Server SSL certificate (path or file content)
     *
     * @param serverSslCert set Server SSL certificate (path or file content)
     * @return this {@link Builder}
     */
    public Builder serverSslCert(String serverSslCert) {
      this.serverSslCert = nullOrEmpty(serverSslCert);
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
     * Key store type
     *
     * @param keyStoreType key store type
     * @return this {@link Builder}
     */
    public Builder keyStoreType(String keyStoreType) {
      this.keyStoreType = nullOrEmpty(keyStoreType);
      return this;
    }

    /**
     * trust store type
     *
     * @param trustStoreType trust store type
     * @return this {@link Builder}
     */
    public Builder trustStoreType(String trustStoreType) {
      this.trustStoreType = nullOrEmpty(trustStoreType);
      return this;
    }

    /**
     * User password
     *
     * @param password password
     * @return this {@link Builder}
     */
    public Builder password(String password) {
      this.password = nullOrEmpty(password);
      return this;
    }

    /**
     * Set ssl protocol list to user (comma separated)
     *
     * @param enabledSslProtocolSuites set possible SSL(TLS) protocol to use
     * @return this {@link Builder}
     */
    public Builder enabledSslProtocolSuites(String enabledSslProtocolSuites) {
      this.enabledSslProtocolSuites = nullOrEmpty(enabledSslProtocolSuites);
      return this;
    }

    /**
     * Set default database
     *
     * @param database database
     * @return this {@link Builder}
     */
    public Builder database(String database) {
      this.database = database;
      return this;
    }

    /**
     * Set failover High-availability mode
     *
     * @param haMode High-availability mode
     * @return this {@link Builder}
     */
    public Builder haMode(HaMode haMode) {
      this._haMode = haMode;
      return this;
    }

    /**
     * Add Host to possible addresses to connect
     *
     * @param host hostname or IP
     * @param port port
     * @return this {@link Builder}
     */
    public Builder addHost(String host, int port) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port));
      return this;
    }

    /**
     * Add Host to possible addresses to connect
     *
     * @param host hostname or IP
     * @param port port
     * @param master is master or replica
     * @return this {@link Builder}
     */
    public Builder addHost(String host, int port, boolean master) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port, master));
      return this;
    }

    /**
     * add host addresses
     *
     * @param hostAddress host addresses
     * @return this {@link Builder}
     */
    public Builder addresses(HostAddress... hostAddress) {
      this._addresses = new ArrayList<>();
      this._addresses.addAll(Arrays.asList(hostAddress));
      return this;
    }

    /**
     * Socket factory
     *
     * @param socketFactory socket factory
     * @return this {@link Builder}
     */
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
     * Indicate if TCP keep-alive must be enabled.
     *
     * @param tcpKeepAlive value
     * @return this {@link Builder}
     */
    public Builder tcpKeepAlive(Boolean tcpKeepAlive) {
      this.tcpKeepAlive = tcpKeepAlive;
      return this;
    }

    /**
     * Indicate if UUID fields must returns as String
     *
     * @param uuidAsString value
     * @return this {@link Builder}
     */
    public Builder uuidAsString(Boolean uuidAsString) {
      this.uuidAsString = uuidAsString;
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
     *   <li>'default' will return org.mariadb.mariadb.jdbc.type Object
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
     * permit to execute an SQL command on connection creation
     *
     * @param initSql initial SQL command
     * @return this {@link Builder}
     */
    public Builder initSql(String initSql) {
      this.initSql = initSql;
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
     * Indicate to compress exchanges with the database through gzip. This permits better
     * performance when the database is not in the same location.
     *
     * @param useCompression to enable/disable compression
     * @return this {@link Builder}
     */
    public Builder useCompression(Boolean useCompression) {
      this.useCompression = useCompression;
      return this;
    }

    /**
     * Set blank table name for metadata (old oracle compatibility)
     *
     * @param blankTableNameMeta use blank table name
     * @return this {@link Builder}
     */
    public Builder blankTableNameMeta(Boolean blankTableNameMeta) {
      this.blankTableNameMeta = blankTableNameMeta;
      return this;
    }

    /**
     * set credential plugin type
     *
     * @param credentialType credential plugin type
     * @return this {@link Builder}
     */
    public Builder credentialType(String credentialType) {
      this.credentialType = nullOrEmpty(credentialType);
      return this;
    }

    /**
     * Set ssl model
     *
     * @param sslMode ssl requirement
     * @return this {@link Builder}
     */
    public Builder sslMode(String sslMode) {
      this.sslMode = sslMode;
      return this;
    }

    /**
     * force default transaction isolation, not using server default
     *
     * @param transactionIsolation indicate default transaction isolation
     * @return this {@link Builder}
     */
    public Builder transactionIsolation(String transactionIsolation) {
      this.transactionIsolation = nullOrEmpty(transactionIsolation);
      return this;
    }

    /**
     * set possible cipher list (comma separated), not using default java cipher list
     *
     * @param enabledSslCipherSuites ssl cipher list
     * @return this {@link Builder}
     */
    public Builder enabledSslCipherSuites(String enabledSslCipherSuites) {
      this.enabledSslCipherSuites = nullOrEmpty(enabledSslCipherSuites);
      return this;
    }

    /**
     * set connection session variables (comma separated)
     *
     * @param sessionVariables session variable list
     * @return this {@link Builder}
     */
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

    /**
     * Year datatype to be considered as date
     *
     * @param yearIsDateType indicate if Year must be considered as Date
     * @return this {@link Builder}
     */
    public Builder yearIsDateType(Boolean yearIsDateType) {
      this.yearIsDateType = yearIsDateType;
      return this;
    }

    /**
     * Force timezone to use this timezone, not default java one
     *
     * @param timezone default timezone
     * @return this {@link Builder}
     */
    public Builder timezone(String timezone) {
      this.timezone = nullOrEmpty(timezone);
      return this;
    }

    /**
     * Must queries be dump on exception stracktrace.
     *
     * @param dumpQueriesOnException must queries be dump on exception
     * @return this {@link Builder}
     */
    public Builder dumpQueriesOnException(Boolean dumpQueriesOnException) {
      this.dumpQueriesOnException = dumpQueriesOnException;
      return this;
    }

    /**
     * If using server prepared statement, set LRU prepare cache size
     *
     * @param prepStmtCacheSize prepare cache size
     * @return this {@link Builder}
     */
    public Builder prepStmtCacheSize(Integer prepStmtCacheSize) {
      this.prepStmtCacheSize = prepStmtCacheSize;
      return this;
    }

    /**
     * Indicate server to return affected rows in place of found rows. This impact the return number
     * of rows affected by update
     *
     * @param useAffectedRows Indicate to user affected rows in place of found rows
     * @return this {@link Builder}
     */
    public Builder useAffectedRows(Boolean useAffectedRows) {
      this.useAffectedRows = useAffectedRows;
      return this;
    }

    /**
     * Indicate to use Client or Server prepared statement
     *
     * @param useServerPrepStmts use Server prepared statement
     * @return this {@link Builder}
     */
    public Builder useServerPrepStmts(Boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    /**
     * Additional connection attributes to identify connection
     *
     * @param connectionAttributes additional connection attributes
     * @return this {@link Builder}
     */
    public Builder connectionAttributes(String connectionAttributes) {
      this.connectionAttributes = nullOrEmpty(connectionAttributes);
      return this;
    }

    /**
     * Use server dedicated bulk batch command
     *
     * @param useBulkStmts use server bulk batch command.
     * @return this {@link Builder}
     */
    public Builder useBulkStmts(Boolean useBulkStmts) {
      this.useBulkStmts = useBulkStmts;
      return this;
    }

    /**
     * Disable pipeline
     *
     * @param disablePipeline disable pipeline.
     * @return this {@link Builder}
     */
    public Builder disablePipeline(Boolean disablePipeline) {
      this.disablePipeline = disablePipeline;
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
     * Permit indicating to force DatabaseMetadata.getDatabaseProductName() to return `MySQL` as
     * database type, not real database type
     *
     * @param useMysqlMetadata force DatabaseMetadata.getDatabaseProductName() to return `MySQL`
     * @return this {@link Builder}
     */
    public Builder useMysqlMetadata(Boolean useMysqlMetadata) {
      this.useMysqlMetadata = useMysqlMetadata;
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
     * On dead-lock exception must add innodb status in exception error message. If enabled, an
     * additional command will be done to retrieve innodb status when dead-lock occurs.
     *
     * @param includeInnodbStatusInDeadlockExceptions Must dead-lock exception must add innodb
     *     status in exception error message
     * @return this {@link Builder}
     */
    public Builder includeInnodbStatusInDeadlockExceptions(
        Boolean includeInnodbStatusInDeadlockExceptions) {
      this.includeInnodbStatusInDeadlockExceptions = includeInnodbStatusInDeadlockExceptions;
      return this;
    }

    /**
     * Dead-lock error will contain threads information
     *
     * @param includeThreadDumpInDeadlockExceptions must dead-lock error contain treads informations
     * @return this {@link Builder}
     */
    public Builder includeThreadDumpInDeadlockExceptions(
        Boolean includeThreadDumpInDeadlockExceptions) {
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
      return this;
    }

    /**
     * set service principal name (GSSAPI)
     *
     * @param servicePrincipalName service principal name (GSSAPI)
     * @return this {@link Builder}
     */
    public Builder servicePrincipalName(String servicePrincipalName) {
      this.servicePrincipalName = nullOrEmpty(servicePrincipalName);
      return this;
    }

    /**
     * Set default fetch size
     *
     * @param defaultFetchSize default fetch size
     * @return this {@link Builder}
     */
    public Builder defaultFetchSize(Integer defaultFetchSize) {
      this.defaultFetchSize = defaultFetchSize;
      return this;
    }

    /**
     * Permit to defined default tls plugin type
     *
     * @param tlsSocketType default tls socket plugin to use
     * @return this {@link Builder}
     */
    public Builder tlsSocketType(String tlsSocketType) {
      this.tlsSocketType = nullOrEmpty(tlsSocketType);
      return this;
    }

    /**
     * Set the log size limit for query
     *
     * @param maxQuerySizeToLog set query size limit
     * @return this {@link Builder}
     */
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

    /**
     * When failover occurs, how many connection attempt before throwing error when reconnecting
     *
     * @param retriesAllDown number of attemps to reconnect
     * @return this {@link Builder}
     */
    public Builder retriesAllDown(Integer retriesAllDown) {
      this.retriesAllDown = retriesAllDown;
      return this;
    }

    /**
     * Indicate galera allowed state (comma separated), permitting to validate if galera node is
     * synchronized
     *
     * @param galeraAllowedState galera allowed state
     * @return this {@link Builder}
     */
    public Builder galeraAllowedState(String galeraAllowedState) {
      this.galeraAllowedState = nullOrEmpty(galeraAllowedState);
      return this;
    }

    /**
     * Create pool if not existing, or get a connection for the pool associate with this connection
     * string if existing.
     *
     * @param pool use pool
     * @return this {@link Builder}
     */
    public Builder pool(Boolean pool) {
      this.pool = pool;
      return this;
    }

    /**
     * set pool name
     *
     * @param poolName pool name
     * @return this {@link Builder}
     */
    public Builder poolName(String poolName) {
      this.poolName = nullOrEmpty(poolName);
      return this;
    }

    /**
     * Set the limit number of connection in pool.
     *
     * @param maxPoolSize maximum connection size in pool.
     * @return this {@link Builder}
     */
    public Builder maxPoolSize(Integer maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    /**
     * Minimum pool size.
     *
     * @param minPoolSize minimum pool size
     * @return this {@link Builder}
     */
    public Builder minPoolSize(Integer minPoolSize) {
      this.minPoolSize = minPoolSize;
      return this;
    }

    /**
     * Set the maximum idle time of a connection indicating that connection must be released
     *
     * @param maxIdleTime maximum idle time of a connection in pool
     * @return this {@link Builder}
     */
    public Builder maxIdleTime(Integer maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
      return this;
    }

    /**
     * Must pool register JMX information
     *
     * @param registerJmxPool register pool to JMX
     * @return this {@link Builder}
     */
    public Builder registerJmxPool(Boolean registerJmxPool) {
      this.registerJmxPool = registerJmxPool;
      return this;
    }

    /**
     * Pool will validate connection before giving it. This amount of time indicate that recently
     * use connection can skip validation 0 means connection will be validated each time (even is
     * just used)
     *
     * @param poolValidMinDelay time limit indicating that connection in pool must be validated
     * @return this {@link Builder}
     */
    public Builder poolValidMinDelay(Integer poolValidMinDelay) {
      this.poolValidMinDelay = poolValidMinDelay;
      return this;
    }

    /**
     * Indicate that connection returned to pool must be RESETed like having proper connection
     * state.
     *
     * @param useResetConnection use reset connection when returning connection to pool.
     * @return this {@link Builder}
     */
    public Builder useResetConnection(Boolean useResetConnection) {
      this.useResetConnection = useResetConnection;
      return this;
    }

    /**
     * MySQL Authentication RSA server file, for mysql authentication
     *
     * @param serverRsaPublicKeyFile server RSA public key file
     * @return this {@link Builder}
     */
    public Builder serverRsaPublicKeyFile(String serverRsaPublicKeyFile) {
      this.serverRsaPublicKeyFile = nullOrEmpty(serverRsaPublicKeyFile);
      return this;
    }

    /**
     * Allow RSA server file retrieval from MySQL server
     *
     * @param allowPublicKeyRetrieval Allow RSA server file retrieval from MySQL server
     * @return this {@link Builder}
     */
    public Builder allowPublicKeyRetrieval(Boolean allowPublicKeyRetrieval) {
      this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
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
              this.uuidAsString,
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
              this.timezone,
              this.dumpQueriesOnException,
              this.prepStmtCacheSize,
              this.useAffectedRows,
              this.useServerPrepStmts,
              this.connectionAttributes,
              this.useBulkStmts,
              this.disablePipeline,
              this.autocommit,
              this.useMysqlMetadata,
              this.createDatabaseIfNotExist,
              this.includeInnodbStatusInDeadlockExceptions,
              this.includeThreadDumpInDeadlockExceptions,
              this.servicePrincipalName,
              this.defaultFetchSize,
              this.tlsSocketType,
              this.maxQuerySizeToLog,
              this.maxAllowedPacket,
              this.retriesAllDown,
              this.galeraAllowedState,
              this.pool,
              this.poolName,
              this.maxPoolSize,
              this.minPoolSize,
              this.maxIdleTime,
              this.registerJmxPool,
              this.poolValidMinDelay,
              this.useResetConnection,
              this.serverRsaPublicKeyFile,
              this.allowPublicKeyRetrieval,
              this.serverSslCert,
              this.keyStore,
              this.keyStorePassword,
              this.keyStoreType,
              this.trustStoreType,
              this.useReadAheadInput,
              this.cachePrepStmts,
              this.transactionReplay,
              this.transactionReplaySize,
              this.geometryDefaultType,
              this.restrictedAuth,
              this.initSql,
              this._nonMappedOptions);
      conf.initialUrl = buildUrl(conf);
      return conf;
    }
  }

  private static String nullOrEmpty(String val) {
    return (val == null || val.isEmpty()) ? null : val;
  }
}
