// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc;

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.CredentialPlugin;
import org.mariadb.jdbc.plugin.credential.CredentialPluginLoader;
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
  private boolean autocommit = true;
  private boolean useMysqlMetadata = false;
  private TransactionIsolation transactionIsolation = TransactionIsolation.REPEATABLE_READ;
  private int defaultFetchSize = 0;
  private int maxQuerySizeToLog = 1024;
  private String geometryDefaultType = null;
  private String restrictedAuth = null;

  // socket
  private String socketFactory = null;
  private int connectTimeout =
      DriverManager.getLoginTimeout() > 0 ? DriverManager.getLoginTimeout() * 1000 : 30_000;
  private String pipe = null;
  private String localSocket = null;
  private boolean tcpKeepAlive = false;
  private int tcpKeepIdle = 0;
  private int tcpKeepCount = 0;
  private int tcpKeepInterval = 0;
  private boolean tcpAbortiveClose = false;
  private String localSocketAddress = null;
  private int socketTimeout = 0;
  private boolean useReadAheadInput = true;
  private String tlsSocketType = null;

  // SSL
  private SslMode sslMode = SslMode.DISABLE;
  private String serverSslCert = null;
  private String keyStore = null;
  private String keyStorePassword = null;
  private String keyStoreType = null;
  private String enabledSslCipherSuites = null;
  private String enabledSslProtocolSuites = null;

  // protocol
  private boolean allowMultiQueries = false;
  private boolean allowLocalInfile = false;
  private boolean useCompression = false;
  private boolean useAffectedRows = false;
  private boolean useBulkStmts = true;

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
      boolean autocommit,
      boolean useMysqlMetadata,
      TransactionIsolation transactionIsolation,
      int defaultFetchSize,
      int maxQuerySizeToLog,
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
      String keyStore,
      String keyStorePassword,
      String keyStoreType,
      String enabledSslCipherSuites,
      String enabledSslProtocolSuites,
      boolean allowMultiQueries,
      boolean allowLocalInfile,
      boolean useCompression,
      boolean useAffectedRows,
      boolean useBulkStmts,
      boolean cachePrepStmts,
      int prepStmtCacheSize,
      boolean useServerPrepStmts,
      CredentialPlugin credentialType,
      String sessionVariables,
      String connectionAttributes,
      String servicePrincipalName,
      boolean blankTableNameMeta,
      boolean tinyInt1isBit,
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
    this.transactionIsolation = transactionIsolation;
    this.defaultFetchSize = defaultFetchSize;
    this.maxQuerySizeToLog = maxQuerySizeToLog;
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
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.keyStoreType = keyStoreType;
    this.enabledSslCipherSuites = enabledSslCipherSuites;
    this.enabledSslProtocolSuites = enabledSslProtocolSuites;
    this.allowMultiQueries = allowMultiQueries;
    this.allowLocalInfile = allowLocalInfile;
    this.useCompression = useCompression;
    this.useAffectedRows = useAffectedRows;
    this.useBulkStmts = useBulkStmts;
    this.cachePrepStmts = cachePrepStmts;
    this.prepStmtCacheSize = prepStmtCacheSize;
    this.useServerPrepStmts = useServerPrepStmts;
    this.credentialType = credentialType;
    this.sessionVariables = sessionVariables;
    this.connectionAttributes = connectionAttributes;
    this.servicePrincipalName = servicePrincipalName;
    this.blankTableNameMeta = blankTableNameMeta;
    this.tinyInt1isBit = tinyInt1isBit;
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
      Boolean yearIsDateType,
      String timezone,
      Boolean dumpQueriesOnException,
      Integer prepStmtCacheSize,
      Boolean useAffectedRows,
      Boolean useServerPrepStmts,
      String connectionAttributes,
      Boolean useBulkStmts,
      Boolean autocommit,
      Boolean useMysqlMetadata,
      Boolean includeInnodbStatusInDeadlockExceptions,
      Boolean includeThreadDumpInDeadlockExceptions,
      String servicePrincipalName,
      Integer defaultFetchSize,
      String tlsSocketType,
      Integer maxQuerySizeToLog,
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
      Boolean useReadAheadInput,
      Boolean cachePrepStmts,
      Boolean transactionReplay,
      Integer transactionReplaySize,
      String geometryDefaultType,
      String restrictedAuth,
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
    if (yearIsDateType != null) this.yearIsDateType = yearIsDateType;
    this.timezone = timezone;
    if (dumpQueriesOnException != null) this.dumpQueriesOnException = dumpQueriesOnException;
    if (prepStmtCacheSize != null) this.prepStmtCacheSize = prepStmtCacheSize;
    if (useAffectedRows != null) this.useAffectedRows = useAffectedRows;
    if (useServerPrepStmts != null) this.useServerPrepStmts = useServerPrepStmts;
    this.connectionAttributes = connectionAttributes;
    if (useBulkStmts != null) this.useBulkStmts = useBulkStmts;
    if (autocommit != null) this.autocommit = autocommit;
    if (useMysqlMetadata != null) this.useMysqlMetadata = useMysqlMetadata;
    if (includeInnodbStatusInDeadlockExceptions != null)
      this.includeInnodbStatusInDeadlockExceptions = includeInnodbStatusInDeadlockExceptions;
    if (includeThreadDumpInDeadlockExceptions != null)
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
    if (servicePrincipalName != null) this.servicePrincipalName = servicePrincipalName;
    if (defaultFetchSize != null) this.defaultFetchSize = defaultFetchSize;
    if (tlsSocketType != null) this.tlsSocketType = tlsSocketType;
    if (maxQuerySizeToLog != null) this.maxQuerySizeToLog = maxQuerySizeToLog;
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
    if (serverSslCert != null) this.serverSslCert = serverSslCert;
    if (keyStore != null) this.keyStore = keyStore;
    if (keyStorePassword != null) this.keyStorePassword = keyStorePassword;
    if (keyStoreType != null) this.keyStoreType = keyStoreType;

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
    return url != null && url.startsWith("jdbc:mariadb:");
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
        String realKey = OptionAliases.OPTIONS_ALIASES.get(keyObj);
        if (realKey == null) realKey = keyObj.toString();
        final Object propertyValue = properties.get(keyObj);

        if (propertyValue != null && realKey != null) {
          try {
            final Field field = Builder.class.getDeclaredField(realKey);
            field.setAccessible(true);
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
                        "Optional parameter %s must be Integer, was '%s'", keyObj, propertyValue));
              }
            }
          } catch (NoSuchFieldException nfe) {
            // keep unknown option:
            // those might be used in authentication or identity plugin
            nonMappedOptions.put(keyObj, propertyValue);
          }
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
        this.transactionIsolation,
        this.defaultFetchSize,
        this.maxQuerySizeToLog,
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
        this.keyStore,
        this.keyStorePassword,
        this.keyStoreType,
        this.enabledSslCipherSuites,
        this.enabledSslProtocolSuites,
        this.allowMultiQueries,
        this.allowLocalInfile,
        this.useCompression,
        this.useAffectedRows,
        this.useBulkStmts,
        this.cachePrepStmts,
        this.prepStmtCacheSize,
        this.useServerPrepStmts,
        this.credentialType,
        this.sessionVariables,
        this.connectionAttributes,
        this.servicePrincipalName,
        this.blankTableNameMeta,
        this.tinyInt1isBit,
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

  public String database() {
    return database;
  }

  public List<HostAddress> addresses() {
    return addresses;
  }

  public HaMode haMode() {
    return haMode;
  }

  public CredentialPlugin credentialPlugin() {
    return credentialType;
  }

  public String user() {
    return user;
  }

  public String password() {
    return password;
  }

  public String initialUrl() {
    return initialUrl;
  }

  public String serverSslCert() {
    return serverSslCert;
  }

  public String keyStore() {
    return keyStore;
  }

  public String keyStorePassword() {
    return keyStorePassword;
  }

  public String keyStoreType() {
    return keyStoreType;
  }

  public String enabledSslProtocolSuites() {
    return enabledSslProtocolSuites;
  }

  public String socketFactory() {
    return socketFactory;
  }

  public int connectTimeout() {
    return connectTimeout;
  }

  public Configuration connectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public String pipe() {
    return pipe;
  }

  public String localSocket() {
    return localSocket;
  }

  public boolean tcpKeepAlive() {
    return tcpKeepAlive;
  }

  public int tcpKeepIdle() {
    return tcpKeepIdle;
  }

  public int tcpKeepCount() {
    return tcpKeepCount;
  }

  public int tcpKeepInterval() {
    return tcpKeepInterval;
  }

  public boolean tcpAbortiveClose() {
    return tcpAbortiveClose;
  }

  public String localSocketAddress() {
    return localSocketAddress;
  }

  public int socketTimeout() {
    return socketTimeout;
  }

  public boolean allowMultiQueries() {
    return allowMultiQueries;
  }

  public boolean allowLocalInfile() {
    return allowLocalInfile;
  }

  public boolean useCompression() {
    return useCompression;
  }

  public boolean blankTableNameMeta() {
    return blankTableNameMeta;
  }

  public SslMode sslMode() {
    return sslMode;
  }

  public TransactionIsolation transactionIsolation() {
    return transactionIsolation;
  }

  public String enabledSslCipherSuites() {
    return enabledSslCipherSuites;
  }

  public String sessionVariables() {
    return sessionVariables;
  }

  public boolean tinyInt1isBit() {
    return tinyInt1isBit;
  }

  public boolean yearIsDateType() {
    return yearIsDateType;
  }

  public String timezone() {
    return timezone;
  }

  public boolean dumpQueriesOnException() {
    return dumpQueriesOnException;
  }

  public int prepStmtCacheSize() {
    return prepStmtCacheSize;
  }

  public boolean useAffectedRows() {
    return useAffectedRows;
  }

  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  public String connectionAttributes() {
    return connectionAttributes;
  }

  public boolean useBulkStmts() {
    return useBulkStmts;
  }

  public boolean autocommit() {
    return autocommit;
  }

  public boolean useMysqlMetadata() {
    return useMysqlMetadata;
  }

  public boolean includeInnodbStatusInDeadlockExceptions() {
    return includeInnodbStatusInDeadlockExceptions;
  }

  public boolean includeThreadDumpInDeadlockExceptions() {
    return includeThreadDumpInDeadlockExceptions;
  }

  public String servicePrincipalName() {
    return servicePrincipalName;
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

  public int retriesAllDown() {
    return retriesAllDown;
  }

  public String galeraAllowedState() {
    return galeraAllowedState;
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

  public String serverRsaPublicKeyFile() {
    return serverRsaPublicKeyFile;
  }

  public boolean allowPublicKeyRetrieval() {
    return allowPublicKeyRetrieval;
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
    private Integer defaultFetchSize;
    private Integer maxQuerySizeToLog;
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
    private String keyStore;
    private String keyStorePassword;
    private String keyStoreType;
    private String enabledSslCipherSuites;
    private String enabledSslProtocolSuites;

    // protocol
    private Boolean allowMultiQueries;
    private Boolean allowLocalInfile;
    private Boolean useCompression;
    private Boolean useAffectedRows;
    private Boolean useBulkStmts;

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

    public Builder user(String user) {
      this.user = nullOrEmpty(user);
      return this;
    }

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

    public Builder tinyInt1isBit(Boolean tinyInt1isBit) {
      this.tinyInt1isBit = tinyInt1isBit;
      return this;
    }

    public Builder yearIsDateType(Boolean yearIsDateType) {
      this.yearIsDateType = yearIsDateType;
      return this;
    }

    public Builder timezone(String timezone) {
      this.timezone = nullOrEmpty(timezone);
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

    public Builder useServerPrepStmts(Boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    public Builder connectionAttributes(String connectionAttributes) {
      this.connectionAttributes = nullOrEmpty(connectionAttributes);
      return this;
    }

    public Builder useBulkStmts(Boolean useBulkStmts) {
      this.useBulkStmts = useBulkStmts;
      return this;
    }

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

    public Builder includeInnodbStatusInDeadlockExceptions(
        Boolean includeInnodbStatusInDeadlockExceptions) {
      this.includeInnodbStatusInDeadlockExceptions = includeInnodbStatusInDeadlockExceptions;
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

    public Builder retriesAllDown(Integer retriesAllDown) {
      this.retriesAllDown = retriesAllDown;
      return this;
    }

    public Builder galeraAllowedState(String galeraAllowedState) {
      this.galeraAllowedState = nullOrEmpty(galeraAllowedState);
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

    public Builder serverRsaPublicKeyFile(String serverRsaPublicKeyFile) {
      this.serverRsaPublicKeyFile = nullOrEmpty(serverRsaPublicKeyFile);
      return this;
    }

    public Builder allowPublicKeyRetrieval(Boolean allowPublicKeyRetrieval) {
      this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
      return this;
    }

    public Builder useReadAheadInput(Boolean useReadAheadInput) {
      this.useReadAheadInput = useReadAheadInput;
      return this;
    }

    public Builder cachePrepStmts(Boolean cachePrepStmts) {
      this.cachePrepStmts = cachePrepStmts;
      return this;
    }

    public Builder transactionReplay(Boolean transactionReplay) {
      this.transactionReplay = transactionReplay;
      return this;
    }

    public Builder transactionReplaySize(Integer transactionReplaySize) {
      this.transactionReplaySize = transactionReplaySize;
      return this;
    }

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
              this.yearIsDateType,
              this.timezone,
              this.dumpQueriesOnException,
              this.prepStmtCacheSize,
              this.useAffectedRows,
              this.useServerPrepStmts,
              this.connectionAttributes,
              this.useBulkStmts,
              this.autocommit,
              this.useMysqlMetadata,
              this.includeInnodbStatusInDeadlockExceptions,
              this.includeThreadDumpInDeadlockExceptions,
              this.servicePrincipalName,
              this.defaultFetchSize,
              this.tlsSocketType,
              this.maxQuerySizeToLog,
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
              this.useReadAheadInput,
              this.cachePrepStmts,
              this.transactionReplay,
              this.transactionReplaySize,
              this.geometryDefaultType,
              this.restrictedAuth,
              this._nonMappedOptions);
      conf.initialUrl = buildUrl(conf);
      return conf;
    }
  }

  private static String nullOrEmpty(String val) {
    return (val == null || val.isEmpty()) ? null : val;
  }
}
